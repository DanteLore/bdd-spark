import cucumber.api.DataTable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.collection.convert.wrapAsScala._

object BddSpark {

  trait DataFrameConvertible[T] {
    def toDF(data: T): DataFrame
  }

  object DataFrameConvertible {
    def apply[T : DataFrameConvertible] : DataFrameConvertible[T] = implicitly
  }

  implicit object DataTableConvertibleToDataFrame extends DataFrameConvertible[DataTable] {

    import Spark._

    override def toDF(data: DataTable): DataFrame = {
      val fieldSpec = getFieldSpec(data)

      val schema = StructType(
        fieldSpec
          .map { case (name, dataType) =>
            StructField(name, dataType, nullable = false)
          }
      )

      val rows = data
        .asMaps(classOf[String], classOf[String])
        .map { row =>
          val values = row
            .values()
            .zip(fieldSpec)
            .map { case (v, (fn, dt)) => (v, dt) }
            .map {
              case (v, DataTypes.IntegerType) => v.toInt
              case (v, DataTypes.DoubleType) => v.toDouble
              case (v, DataTypes.LongType) => v.toLong
              case (v, DataTypes.BooleanType) => v.toBoolean
              case (v, DataTypes.StringType) => v
            }
            .toSeq

          Row.fromSeq(values)
        }
        .toList

      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      df
    }

    def getFieldSpec(data: DataTable): List[(String, DataType)] = {
      val fieldSpec = data
        .topCells()
        .map(_.split(':'))
        .map(splits => (splits(0).trim, splits(1).toLowerCase.trim))
        .map {
          case (name, "string") => (name, DataTypes.StringType)
          case (name, "double") => (name, DataTypes.DoubleType)
          case (name, "int") => (name, DataTypes.IntegerType)
          case (name, "integer") => (name, DataTypes.IntegerType)
          case (name, "long") => (name, DataTypes.LongType)
          case (name, "boolean") => (name, DataTypes.BooleanType)
          case (name, "bool") => (name, DataTypes.BooleanType)
          case (name, _) => (name, DataTypes.StringType)
        }
      fieldSpec.toList
    }
  }

  implicit class DataFrameConvertibleOps[T : DataFrameConvertible](data : T) {
    def toDF() : DataFrame = DataFrameConvertible[T].toDF(data)
  }
}
