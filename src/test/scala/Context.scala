object Context {
  var calculator : (Int, Int) => Int = _
  var result = 0

  var parquetFilename = ""
  var savedData = Spark.sqlContext.emptyDataFrame

  var files = Map[String, String]()
}
