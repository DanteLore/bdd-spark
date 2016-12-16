import org.apache.spark.sql.functions._

object HousePrices {
  def countRows(tableName: String): Long = {
    Spark.sqlContext.sql(s"select * from $tableName").count()
  }

  def doItByPostcode(input: String, postcode: String, output: String): Unit = {
    Spark.sqlContext.sql(s"select year, avg(price) as averageHousePrice, min(price) as minHousePrice, max(price) as maxHousePrice from $input where postcode = '$postcode' group by year").registerTempTable(output)
  }

  def doItWithMinMax(input: String, output: String): Unit = {
    Spark.sqlContext.sql(s"select year, avg(price) as averageHousePrice, min(price) as minHousePrice, max(price) as maxHousePrice from $input group by year").registerTempTable(output)
  }

  def justDoIt(input: String, output: String): Unit = {
    Spark.sqlContext.sql(s"select year,avg(price) as averageHousePrice from $input group by year").registerTempTable(output)
  }
  def toYear = udf((s : String) => s.substring(0, 4))
}
