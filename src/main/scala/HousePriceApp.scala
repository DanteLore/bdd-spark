import org.apache.spark
import org.apache.spark.sql.functions._

object HousePriceApp {
  def toYear = udf((s : String) => s.substring(0, 4))

  def main(args: Array[String]): Unit = {

    Spark.sqlContext
      .read
      .parquet("/Users/pcoward/Documents/house_price/house_price")
        .withColumn("year", toYear(col("date")))
      .registerTempTable("house_prices")

    HousePrices.doItByPostcode("house_prices", "RG18 4EX", "poo")

    Spark.sqlContext.sql("select * from poo").foreach(println)

  }
}
