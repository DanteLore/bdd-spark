import org.apache.spark.sql.DataFrame

trait ParquetWriter {
  def write(df: DataFrame, path: String)
}

object ParquetWriter {
  class RealParquetWriter extends ParquetWriter {
    override def write(df: DataFrame, path: String): Unit = {
      df.write.format("parquet").save(path)
    }
  }

  def apply(): ParquetWriter = {
    new RealParquetWriter
  }
}
