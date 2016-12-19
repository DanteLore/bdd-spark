import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object HousePrices {
  import Spark._

  private def toYear = udf((s : String) => s.substring(0, 4))

  def getDataWithYear(input : String) : DataFrame = {
    spark.sql(s"select * from $input")
        .withColumn("year", toYear(col("date")))
  }

  def countRows(tableName: String): Long = {
    spark
      .sql(s"select count(*) from $tableName")
      .collect
      .head
      .getLong(0)
  }

  def doItByPostcode(input: String, postcode: String, output: String): Unit = {
    getDataWithYear(input).createOrReplaceTempView("with_year")

    spark.sql(s"select year, avg(price) as averageHousePrice, min(price) as minHousePrice, max(price) as maxHousePrice from with_year where postcode = '$postcode' group by year")
      .createOrReplaceTempView(output)
  }

  def doItWithMinMax(input: String, output: String): Unit = {
    getDataWithYear(input).createOrReplaceTempView("with_year")

    spark.sql(s"select year, avg(price) as averageHousePrice, min(price) as minHousePrice, max(price) as maxHousePrice from with_year group by year")
      .createOrReplaceTempView(output)
  }

  def justDoIt(input: String, output: String): Unit = {
    getDataWithYear(input).createOrReplaceTempView("with_year")

    spark.sql(s"select year,avg(price) as averageHousePrice from with_year group by year")
      .createOrReplaceTempView(output)
  }
}
