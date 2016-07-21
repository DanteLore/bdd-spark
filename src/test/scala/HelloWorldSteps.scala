import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers

class HelloWorldSteps extends ScalaDsl with EN with Matchers {


  def countWords(words: String): Int = {
    words.split(" ").length
  }


  def countRomeos(words: String): Int = {

    words.map(_.toLower).split("[^a-z]").count(_ == "romeo")

  }

  def averageLineRomeo(words: String): Int = {

    val numberOfRomeos = words.map(_.toLower).split("\n").map(_.split("[^a-z]")).map(_.count(_ == "romeo")).reduce(_ + _)

    val numberOfLines = words.split("[\n]").length

    numberOfRomeos / numberOfLines


  }


  Given("""^a calculator$""") { () =>
    Context.calculator = (x: Int, y: Int) => {
      x + y
    }
  }

  When("""^I add '(\d+)' and '(\d+)'$""") { (x: Int, y: Int) =>
    Context.result = Context.calculator(x, y)
  }

  Then("""^the result is '(\d+)'$""") { (expected: Int) =>
    Context.result shouldEqual expected
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

    Context.result = Context.files("shakespeare").split("\n").filter(f => f.contains(":")).map(f => f.split(":")(1).split(" ").filterNot(_.isEmpty).length).reduce(_ + _)
  }
}
