import org.apache.spark.sql.DataFrame

object HousePriceDataBusinessLogic {
  def processHousePrices(housePrices : DataFrame, postcodes : DataFrame) : DataFrame = {
    housePrices.join(postcodes, "Postcode")
  }
}
