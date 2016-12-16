object Context {
  var calculator : (Int, Int) => Int = _
  var result :Long = 0

  var parquetFilename = ""
  var savedData = Spark.sqlContext.emptyDataFrame

  var files = Map[String, String]()
}
