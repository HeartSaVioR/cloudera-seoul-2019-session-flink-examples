package cloudera.session.seoul.flink.examples.chainedaggregations

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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
