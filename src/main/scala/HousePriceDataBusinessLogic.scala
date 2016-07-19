import org.apache.spark.sql.DataFrame

object HousePriceDataBusinessLogic {
  def processHousePrices(housePrices : DataFrame, postcodes : DataFrame) : DataFrame = {
    housePrices.join(postcodes, "Postcode")
  }

  def processHousePricesAndSaveToParquet(housePrices : DataFrame, postcodes : DataFrame, parquetWriter: ParquetWriter) = {
    parquetWriter.write(housePrices.join(postcodes, "Postcode"), "results.parquet")
  }
}
