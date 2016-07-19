import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object HousePriceDataBusinessLogic {
  def processDataFromFilesAndSaveToParquet(reader: FileReader, priceFilename: String, postcodeFileName: String, writer: ParquetWriter) = {
    val priceSchema = StructType(Seq(
      StructField("Price", DataTypes.IntegerType),
      StructField("Postcode", DataTypes.StringType),
      StructField("HouseType", DataTypes.StringType)
    ))

    val prices = reader
      .readFile(priceFilename)
      .map(_.split(','))
      .map(row => row.map(_.trim()))
      .map(splits => Row(splits(0).toInt, splits(1), splits(2)))

    val priceDf = Spark.sqlContext.createDataFrame(prices, priceSchema)

    val postcodeSchema = StructType(Seq(
      StructField("Postcode", DataTypes.StringType),
      StructField("Latitude", DataTypes.DoubleType),
      StructField("Longitude", DataTypes.DoubleType)
    ))

    val postcodes = reader
      .readFile(postcodeFileName)
      .map(_.split(','))
      .map(row => row.map(_.trim()))
      .map(splits => Row(splits(0), splits(1).toDouble, splits(2).toDouble))

    val postcodeDf = Spark.sqlContext.createDataFrame(postcodes, postcodeSchema)

    val joined = priceDf.join(postcodeDf, "Postcode")

    writer.write(joined, "results.parquet")
  }

  def processHousePrices(housePrices : DataFrame, postcodes : DataFrame) : DataFrame = {
    housePrices.join(postcodes, "Postcode")
  }

  def processHousePricesAndSaveToParquet(housePrices : DataFrame, postcodes : DataFrame, parquetWriter: ParquetWriter) = {
    parquetWriter.write(housePrices.join(postcodes, "Postcode"), "results.parquet")
  }
}
