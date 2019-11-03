package cloudera.session.seoul.flink.examples.chainedaggregations

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

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

// max 10 mins out of order
class CardTransactionTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[CardTransaction](Time.minutes(10)) {
  override def extractTimestamp(t: CardTransaction): Long = {
    println(s"card transaction> $t")
    t.timestamp
  }
}
