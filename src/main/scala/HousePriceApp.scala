
// Yeah yeah, normally you wouldn't have a "main" for a spark app, but you get the idea.

object HousePriceApp {
  def main(args: Array[String]): Unit = {

    import Spark._

    spark.sqlContext
      .read
      .parquet("/Users/DTAYLOR/Data/house_price/parquet")
      .createOrReplaceTempView("house_prices")

    HousePrices.doItByPostcode("house_prices", "RG18 4EX", "poo")

    spark.sql("select * from poo").foreach(x => println(x))
  }
}
