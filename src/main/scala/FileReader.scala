import org.apache.spark.rdd.RDD

trait FileReader {
  def readFile(filename : String) : RDD[String]
}

object FileReader {
  class RealFileReader extends FileReader{
    override def readFile(filename: String): RDD[String] = {
      Spark.sc.textFile(filename)
    }
  }

  def apply() = new RealFileReader
}
