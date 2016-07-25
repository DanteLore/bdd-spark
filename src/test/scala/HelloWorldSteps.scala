import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers

class HelloWorldSteps extends ScalaDsl with EN with Matchers {

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
}


