package cloudera.session.seoul.flink.examples.chainedaggregations

import java.lang.{Long => JLong}
import java.sql.Timestamp
import java.util
import java.util.{Collections, UUID}

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp}
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{TimestampKind, TimestampType}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

case class CardTransactionForTable(transactionId: String, province: String, city: String,
                                   amount: Long, timestamp: java.sql.Timestamp)

class CardTransactionSourceForTable extends RichSourceFunction[Row] {
  private def convert(ct: CardTransaction): Row = {
    Row.of(ct.transactionId.toString, ct.province, ct.city, new JLong(ct.amount),
      new java.sql.Timestamp(ct.timestamp))
  }

  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    // 0 ~ 10 mins (first window)
    ctx.collect(convert(CardTransaction("Seoul", "Seoul", 9000, 0)))
    ctx.collect(convert(CardTransaction("Seoul", "Seoul", 13000, 1)))
    ctx.collect(convert(CardTransaction("Seoul", "Seoul", 150000, 5)))
    ctx.collect(convert(CardTransaction("Seoul", "Seoul", 2000000, 9)))

    // 0 ~ 30 mins (various windows in first group, but same window in second group)

    // below twos are in a same window in first group
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 12000, 9)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 27000, 5)))
    // below twos are not a same window in first group
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 156000, 15)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 900000, 27)))

    ctx.collect(convert(CardTransaction("Gyeonggi", "Seongnam", 130000, 15)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Seongnam", 1100000, 25)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Bucheon", 300000, 5)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Bucheon", 20000, 8)))

    // 31 ~ 60 mins (different windows than above)

    // below twos are in a same window in first group
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 12000, 31)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 27000, 35)))
    // below twos are not a same window in first group
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 156000, 35)))
    ctx.collect(convert(CardTransaction("Gyeonggi", "Suji", 900000, 55)))

    // force emit watermark to see result immediately (2hr)
    ctx.emitWatermark(new Watermark(2 * 60 * 60 * 1000))

    // need to sleep enough time
    Thread.sleep(1000 * 60)

    // Flink will finish the application after run() method returns
  }

  override def cancel(): Unit = {}
}

// max 10 mins out of order
class CardTransactionForTableTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[Row](Time.minutes(10)) {
  override def extractTimestamp(r: Row): Long = {
    println(s"card transaction> $r")
    r.getField(4).asInstanceOf[java.sql.Timestamp].getTime
  }
}

class CardTransactionTableSource extends StreamTableSource[Row] with DefinedRowtimeAttributes {
  override def getDataStream(execEnv: environment.StreamExecutionEnvironment): DataStream[Row] = {
    execEnv
      .addSource(new CardTransactionSourceForTable, getReturnType)
      .setParallelism(1)
      // we can let our source to set timestamp and emit watermark, but worth to know
      .assignTimestampsAndWatermarks(new CardTransactionForTableTimeAssigner)
  }

  override def getTableSchema: TableSchema = TableSchema.builder()
    .field("transactionId", DataTypes.STRING())
    .field("province", DataTypes.STRING())
    .field("city", DataTypes.STRING())
    .field("amount", DataTypes.BIGINT())

    // TODO: This hack seems to be the only way to make sure "timestamp" field can be used as event-time field.
    //  - FLINK-14322 seems to resolve this, but not released yet.
    // TODO: This conflicts with TableSourceUtil.computeIndexMapping
    //  - rowtime field should be java.sql.Timestamp, but TimestampType converts to LocalDateTime
    //  - bridgedTo(classOf[Timestamp]) would avoid this
    //  - referred https://github.com/apache/flink/blob/15f8f3c52a1bf11ecf9f550388eee550b7fc763e/flink-table/flink-table-common/src/test/java/org/apache/flink/table/descriptors/DescriptorPropertiesTest.java#L174-L187
    // TODO: ... but adding 'bridgedTo' leads canConvertToTimeAttributeTypeInfo(dataType) to 'false' so
    //    convertToTimeAttributeTypeInfo is not done, and finally window('timestamp) doesn't recognize
    //    'timestamp as rowtime
    .field("timestamp",
      new AtomicDataType(new TimestampType(false, TimestampKind.ROWTIME, 3))
        .bridgedTo(classOf[Timestamp])
    )
//    .field("timestamp", DataTypes.TIMESTAMP())
    .build()

  override def getReturnType: TypeInformation[Row] = {
    val names = Array[String]("transactionId" , "province", "city", "amount", "timestamp")
    val types = Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.SQL_TIMESTAMP)
    Types.ROW_NAMED(names, types: _*)
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    val rowtimeAttrDescr = new RowtimeAttributeDescriptor(
      "timestamp",
      new ExistingField("timestamp"),
      new PreserveWatermarks())
    Collections.singletonList(rowtimeAttrDescr)
  }
}
