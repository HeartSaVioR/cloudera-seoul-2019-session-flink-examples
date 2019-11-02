package cloudera.session.seoul.flink.examples.chainedaggregations

import java.util.UUID

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ChainedStreamingAggregationDataStreamExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val events: DataStream[CardTransaction] = env
      .addSource(new CardTransactionSource)
      .setParallelism(1)
      // we can let our source to set timestamp and emit watermark, but worth to know
      .assignTimestampsAndWatermarks(new CardTransactionTimeAssigner)

    val groupedByProvinceAndCity = events
      .keyBy(ct => (ct.province, ct.city, TransactionAmountGroup.group(ct.amount)))
      .timeWindow(Time.minutes(10))
      .aggregate(new CardTransactionAggregateFunction, new MyProcessWindowFunction())

    groupedByProvinceAndCity.print("groupedByProvinceAndCity")

    val groupedByProvince = groupedByProvinceAndCity
      .keyBy(agg => (agg.province, agg.amountGroup))
      .timeWindow(Time.minutes(30))
      .aggregate(new CardTransactionWithWindowAggregateFunction, new MyProcessWindowFunction2())

    groupedByProvince.print("groupedByProvince")

    env.execute("Chained Streaming Aggregations")
  }
}

class CardTransactionAggregateFunction
  extends AggregateFunction[CardTransaction, CardTransactionsAggAccumulator, CardTransactionsAggregated] {
  override def createAccumulator(): CardTransactionsAggAccumulator = new CardTransactionsAggAccumulator()

  override def add(value: CardTransaction, accumulator: CardTransactionsAggAccumulator): CardTransactionsAggAccumulator = {
    accumulator.count += 1
    accumulator.sum += value.amount
    accumulator
  }

  override def getResult(accumulator: CardTransactionsAggAccumulator): CardTransactionsAggregated = {
    CardTransactionsAggregated(accumulator.count, accumulator.sum)
  }

  override def merge(a: CardTransactionsAggAccumulator, b: CardTransactionsAggAccumulator): CardTransactionsAggAccumulator = {
    new CardTransactionsAggAccumulator(a.count + b.count, a.sum + b.sum)
  }
}

class CardTransactionWithWindowAggregateFunction
  extends AggregateFunction[CardTransactionsAggregatedWithWindow, CardTransactionsAggAccumulator, CardTransactionsAggregated] {
  override def createAccumulator(): CardTransactionsAggAccumulator = new CardTransactionsAggAccumulator()

  override def add(value: CardTransactionsAggregatedWithWindow, accumulator: CardTransactionsAggAccumulator): CardTransactionsAggAccumulator = {
    accumulator.count += value.count
    accumulator.sum += value.sum
    accumulator
  }

  override def getResult(accumulator: CardTransactionsAggAccumulator): CardTransactionsAggregated = {
    CardTransactionsAggregated(accumulator.count, accumulator.sum)
  }

  override def merge(a: CardTransactionsAggAccumulator, b: CardTransactionsAggAccumulator): CardTransactionsAggAccumulator = {
    new CardTransactionsAggAccumulator(a.count + b.count, a.sum + b.sum)
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[
  CardTransactionsAggregated,
  CardTransactionsAggregatedWithWindow,
  (String, String, TransactionAmountGroup.Value),
  TimeWindow] {

  def process(
      key: (String, String, TransactionAmountGroup.Value),
      context: Context,
      values: Iterable[CardTransactionsAggregated],
      out: Collector[CardTransactionsAggregatedWithWindow]): Unit = {
    val value = values.iterator.next()
    out.collect(CardTransactionsAggregatedWithWindow(key._1, key._2,
      key._3, context.window.getStart, context.window.getEnd,
      value.count, value.sum))
  }
}

class MyProcessWindowFunction2 extends ProcessWindowFunction[
  CardTransactionsAggregated,
  CardTransactionsAggregatedWithWindow,
  (String, TransactionAmountGroup.Value),
  TimeWindow] {

  def process(
      key: (String, TransactionAmountGroup.Value),
      context: Context,
      values: Iterable[CardTransactionsAggregated],
      out: Collector[CardTransactionsAggregatedWithWindow]): Unit = {
    val value = values.iterator.next()
    out.collect(CardTransactionsAggregatedWithWindow(key._1, "__ALL__",
      key._2, context.window.getStart, context.window.getEnd,
      value.count, value.sum))
  }
}

// max 10 mins out of order
class CardTransactionTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[CardTransaction](Time.minutes(10)) {
  override def extractTimestamp(t: CardTransaction): Long = {
    println(s"card transaction> $t")
    t.timestamp
  }
}

case class CardTransaction(
    transactionId: UUID,
    province: String,
    city: String,
    amount: Long,
    timestamp: Long)

class CardTransactionsAggAccumulator(var count: Long = 0, var sum: Long = 0)

case class CardTransactionsAggregated(count: Long, sum: Long)

case class CardTransactionsAggregatedWithWindow(
    province: String,
    city: String,
    amountGroup: TransactionAmountGroup.Value,
    windowStart: Long,
    windowEnd: Long,
    count: Long,
    sum: Long)

object TransactionAmountGroup extends Enumeration {
  // A: ~ 9999
  // B: 10000 ~ 99999
  // C: 100000 ~ 999999
  // D: 1000000 ~
  val A, B, C, D = Value

  def group(amount: Long): TransactionAmountGroup.Value = {
    if (amount < 10000) A
    else if (amount >= 10000 && amount < 100000) B
    else if (amount >= 100000 && amount < 1000000) C
    else D
  }
}

object CardTransaction {
  def apply(province: String, city: String, amount: Long, tsMins: Long): CardTransaction = {
    CardTransaction(UUID.randomUUID(), province, city, amount, minutesToMillis(tsMins))
  }

  private def minutesToMillis(min: Long): Long = {
    Time.minutes(min).toMilliseconds
  }
}

class CardTransactionSource extends RichSourceFunction[CardTransaction] {
  override def run(srcCtx: SourceContext[CardTransaction]): Unit = {

    // 0 ~ 10 mins (first window)
    srcCtx.collect(CardTransaction("Seoul", "Seoul", 9000, 0))
    srcCtx.collect(CardTransaction("Seoul", "Seoul", 13000, 1))
    srcCtx.collect(CardTransaction("Seoul", "Seoul", 150000, 5))
    srcCtx.collect(CardTransaction("Seoul", "Seoul", 2000000, 9))

    // 0 ~ 30 mins (various windows in first group, but same window in second group)

    // below twos are in a same window in first group
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 12000, 9))
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 27000, 5))
    // below twos are not a same window in first group
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 156000, 15))
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 900000, 27))

    srcCtx.collect(CardTransaction("Gyeonggi", "Seongnam", 130000, 15))
    srcCtx.collect(CardTransaction("Gyeonggi", "Seongnam", 1100000, 25))
    srcCtx.collect(CardTransaction("Gyeonggi", "Bucheon", 300000, 5))
    srcCtx.collect(CardTransaction("Gyeonggi", "Bucheon", 20000, 8))

    // 31 ~ 60 mins (different windows than above)

    // below twos are in a same window in first group
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 12000, 31))
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 27000, 35))
    // below twos are not a same window in first group
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 156000, 35))
    srcCtx.collect(CardTransaction("Gyeonggi", "Suji", 900000, 55))

    // force emit watermark to see result immediately (2hr)
    srcCtx.emitWatermark(new Watermark(2 * 60 * 60 * 1000))

    // need to sleep enough time
    Thread.sleep(1000 * 60)

    // Flink will finish the application after run() method returns
  }

  override def cancel(): Unit = {}
}
