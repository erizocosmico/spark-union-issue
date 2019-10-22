package co.erizocosmi.sparkunionissue

import java.util

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DefaultSource {
  val Name = "co.erizocosmi.sparkunionissue"

  private val products = Seq(
    Row("candy", 1),
    Row("chocolate", 2),
    Row("milk", 3),
    Row("cinnamon", 4),
    Row("pizza", 5),
    Row("pineapple", 6)
  )

  private val users = Seq(
    Row("andy", 1),
    Row("alice", 2),
    Row("mike", 3),
    Row("mariah", 4),
    Row("eleanor", 5),
    Row("matthew", 6)
  )
}

class DefaultSource extends DataSourceV2 with ReadSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    options.get("table").orElse("") match {
      case "users" => DefaultReader(DefaultSource.users)
      case "products" => DefaultReader(DefaultSource.products)
      case t => throw new SparkException(s"invalid table '$t'")
    }
  }

}

case class DefaultReader(rows: Seq[Row]) extends DataSourceReader {
  override def readSchema(): StructType = StructType(Seq(
    StructField("name", StringType),
    StructField("id", IntegerType)
  ))

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val result = new java.util.ArrayList[DataReaderFactory[Row]]()
    result.add(DefaultDataReaderFactory(rows.take(rows.length / 2)))
    result.add(DefaultDataReaderFactory(rows.drop(rows.length / 2)))
    result
  }
}

case class DefaultDataReaderFactory(rows: Seq[Row]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    DefaultDataReader(rows)
  }
}

case class DefaultDataReader(rows: Seq[Row]) extends DataReader[Row] {
  private val it = rows.iterator

  override def next(): Boolean = it.hasNext
  override def get(): Row = it.next
  override def close(): Unit = {}
}