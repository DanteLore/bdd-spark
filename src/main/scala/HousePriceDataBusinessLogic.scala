import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object HousePriceDataBusinessLogic {
  def processHousePrices(housePrices : DataFrame, postcodes : DataFrame) : DataFrame = {
    housePrices.join(postcodes, "Postcode")
  }

  def processHousePricesAndSaveToParquet(housePrices : DataFrame, postcodes : DataFrame, parquetWriter: ParquetWriter) : Unit = {
    parquetWriter.write(housePrices.join(postcodes, "Postcode"), "results.parquet")
  }

  def processDataFromFilesFilterItThenSaveItToParquet(reader: FileReader, geoFilename : String, priceFilename: String, postcodeFileName: String, writer: ParquetWriter) : Unit = {
    val joined = loadAndJoin(reader, priceFilename, postcodeFileName)

    // If this was real code, a geoJSON library would be sensible here. Dirty code follows:
    val json = parse(reader.readText(geoFilename))\\ "coordinates"
    val coords = json match {
      case JArray(outer) => outer.map{ case JArray(inner) => inner }
    }
    val points =
      coords
        .map(c => (c(0), c(1)))
        .map{ case (JDouble(long), JDouble(lat)) => (long, lat) }

    val minLat = Math.min(points(0)._2, points(1)._2)
    val maxLat = Math.max(points(0)._2, points(1)._2)
    val minLong = Math.min(points(0)._1, points(1)._1)
    val maxLong = Math.max(points(0)._1, points(1)._1)

    val filtered = joined
      .filter(s"Latitude >= $minLat and Latitude <= $maxLat")
      .filter(s"Longitude >= $minLong and Longitude <= $maxLong")

    writer.write(filtered, "results.parquet")
  }

  def processDataFromFilesAndSaveToParquet(reader: FileReader, priceFilename: String, postcodeFileName: String, writer: ParquetWriter) : Unit = {
    val joined = loadAndJoin(reader, priceFilename, postcodeFileName)

    writer.write(joined, "results.parquet")
  }

  private def loadAndJoin(reader: FileReader, priceFilename: String, postcodeFileName: String): DataFrame = {
    val priceSchema = StructType(Seq(
      StructField("Price", DataTypes.IntegerType),
      StructField("Postcode", DataTypes.StringType),
      StructField("HouseType", DataTypes.StringType)
    ))

    val prices = reader
      .readLinesToRdd(priceFilename)
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
      .readLinesToRdd(postcodeFileName)
      .map(_.split(','))
      .map(row => row.map(_.trim()))
      .map(splits => Row(splits(0), splits(1).toDouble, splits(2).toDouble))

    val postcodeDf = Spark.sqlContext.createDataFrame(postcodes, postcodeSchema)

    val joined = priceDf.join(postcodeDf, "Postcode")
    joined
  }
}
