import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers

class ShakespeareSteps extends ScalaDsl with EN with Matchers {
  def countWords(words: String): Int = {
    words.split(" ").length
  }

  def countRomeos(words: String): Int = {
    words.map(_.toLower).split("[^a-z]").count(_ == "romeo")
  }

  def averageLineRomeo(words: String): Int = {
    val numberOfRomeos = words.map(_.toLower).split("\n").map(_.split("[^a-z]")).map(_.count(_ == "romeo")).sum

    val numberOfLines = words.split("[\n]").length

    numberOfRomeos / numberOfLines
  }

  def averageWordsPerLine(reader: FileReader, filename: String, word: String): Int = {
    // The more obvious lines.sum(...) / lines.count(...) above means iterating twice. Which isn't very Sparky!

    val x = reader
      .readLinesToRdd(filename)
      .map(_.toLowerCase())
      .map { line =>
        line.split("\\W").count(_ == word)
      }
      .map((_, 1))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    x._1 / x._2
  }

  def countSpokenWordsDanStyle(reader : FileReader, filename : String) : Long = {
    val pattern = "^.*:(.*)$".r

    reader
      .readLinesToRdd(filename)
      .filter(x => x.matches("^.+:.*"))
      .map { case pattern(c) => c}
      .flatMap(_.split("\\s"))
      .filter(_.length > 0)
      .count()
  }

  def generateWordCounts(reader: FileReader, filename: String, tablename: String): Unit = {
    import Spark.sqlContext.implicits._

    val pattern = "^.*:(.*)$".r

    reader
      .readLinesToRdd(filename)
      .filter(x => x.matches("^.+:.*"))
      .map { case pattern(c) => c}
      .flatMap(_.split("\\W"))
      .filter(_.length > 0)
      .map(_.toLowerCase)
      .map((_, 1))
      .reduceByKey(_ + _)
      .toDF("word", "count")
      .registerTempTable(tablename)
  }

  When("""^I count the words$""") { () =>
    val words = Context.files("shakespeare")
    Context.result = countWords(words)

  }

  When("""^I count the romeos$""") { () =>
    val words = Context.files("shakespeare")
    Context.result = countRomeos(words)
  }

  When("""^I average the romeos$""") { () =>
    val words = Context.files("shakespeare");
    Context.result = averageLineRomeo(words)
  }

  When("""^I count spoken words$""") { () =>
    Context.result = Context.files("shakespeare")
      .split("\n")
      .filter(f => f.contains(":"))
      .map(f => f.split(":")(1)
        .split(" ")
        .filterNot(_.isEmpty).length).sum
  }

  When("""^I count spoken words Dan style from file "([^"]*)"$"""){ (filename:String) =>
    // Inject the file reader - so it's like production code!
    Context.result = countSpokenWordsDanStyle(new MockFileReader(), filename).toInt
  }

  When("""^I calculate the average "([^"]*)" count per line in "([^"]*)"$"""){ (word:String, filename:String) =>
    Context.result = averageWordsPerLine(new MockFileReader(), filename, word)
  }

  When("""^I count the spoken words in "([^"]*)" saving results to table "([^"]*)"$"""){ (filename:String, tablename:String) =>
    generateWordCounts(new MockFileReader(), filename, tablename)
  }
}
