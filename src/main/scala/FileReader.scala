import org.apache.spark.rdd.RDD

trait FileReader {
  def readLinesToRdd(filename : String) : RDD[String]
  def readText(filename : String) : String
}

object FileReader {
  class RealFileReader extends FileReader{
    override def readLinesToRdd(filename: String): RDD[String] = {
      Spark.sc.textFile(filename)
    }

    override def readText(filename: String): String = {
      //Whatever!
      ""
    }
  }

  def apply() : FileReader = new RealFileReader
}
