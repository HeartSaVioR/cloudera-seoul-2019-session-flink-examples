package cloudera.session.seoul.flink.examples.chainedaggregations

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

// FIXME: This is not working - maybe I'm missing here, or there're some bugs or unhandled things in Flink

object ChainedStreamingAggregationTableApiExample {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.getConfig.setAutoWatermarkInterval(1000L)

    val tableEnv = StreamTableEnvironment.create(streamEnv)
    val amountGroupFn = new TransactionAmountGroupUDF

    val groupedByProvinceAndCity = tableEnv
      .fromTableSource(new CardTransactionTableSource)
      .select('province, 'city, amountGroupFn('amount).as('amountGroup),
        'amount, 'timestamp)
      .window(Tumble over 10.minutes on 'timestamp as 'w)
      .groupBy('province, 'city, 'amountGroup, 'w)
      .select('w.start as 'window_start, 'w.end as 'window_end,
        'province, 'city, 'amountGroup, 'province.count as 'count, 'amount.sum as 'sum)

    // TODO: explain throws exception... sounds like a bug from either my code or Flink
    /*
    Exception in thread "main" java.lang.RuntimeException: Error while applying rule PushProjectIntoTableSourceScanRule, args [rel#70:FlinkLogicalCalc.LOGICAL(input=RelSubset#40,expr#0..4={inputs},expr#5=cloudera$session$seoul$flink$examples$chainedaggregations$TransactionAmountGroupUDF$5285fe73fd156233268f81b47f7d8bf8($t3),province=$t1,city=$t2,amountGroup=$t5,amount=$t3,timestamp=$t4), Scan(table:[unregistered_209845522], fields:(transactionId, province, city, amount, timestamp), source:CardTransactionTableSource(transactionId, province, city, amount, timestamp))]
      ...
    Caused by: org.apache.flink.table.api.ValidationException: Rowtime field 'timestamp' has invalid type LocalDateTime. Rowtime attributes must be of type Timestamp.
     */

//    tableEnv.explain(groupedByProvinceAndCity)

    // TODO: throws exception... sounds like a bug from either my code or Flink... see the comments in CardTransactionTableSource
    // Exception in thread "main" java.lang.UnsupportedOperationException: Event-time grouping windows on row intervals are currently not supported.
    implicit val typeInfo = TypeInformation.of(classOf[GroupedByProvinceAndCityResult])
    groupedByProvinceAndCity.toAppendStream[GroupedByProvinceAndCityResult].print("groupedByProvinceAndCity")

    // TODO: After window grouping, which column would retain the 'rowtime attribute'? next window grouping
    //  would require the 'rowtime attribute' column to be used for window.
/*
    val groupedByProvince = groupedByProvinceAndCity
      .window(Tumble over 30.minutes on 'window_start as 'w)
      .groupBy('province, 'amountGroup, 'w)
      // TODO: this throws exception... looks like same issue on CardTransactionTableSource.getTableSchema
      //  Exception in thread "main" org.apache.flink.table.api.ValidationException: A group window expects a time attribute for grouping in a stream environment.
      .select('w.start as 'window_start, 'w.end as 'window_end, 'province,
        'amountGroup, 'count.sum as 'count, 'sum.sum as 'sum)

    implicit val typeInfo2 = TypeInformation.of(classOf[GroupedByProvinceResult])
    groupedByProvince.toAppendStream[GroupedByProvinceResult].print("groupedByProvince")
 */

    tableEnv.execute("Chained Streaming Aggregations by Table API")
  }
}

class TransactionAmountGroupUDF extends ScalarFunction {
  def eval(amount: Long): String = {
    TransactionAmountGroup.group(amount).toString
  }
}

case class GroupedByProvinceAndCityResult(
    windowStart: Long, windowEnd: Long, province: String, city: String,
    amountGroup: String, count: Long, sum: Long)

case class GroupedByProvinceResult(
    windowStart: Long, windowEnd: Long, province: String, amountGroup: String,
    count: Long, sum: Long)
