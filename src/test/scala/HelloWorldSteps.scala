import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers

class HelloWorldSteps extends ScalaDsl with EN with Matchers {

  var calculator : (Int, Int) => Int = _
  var result = 0

  Given("""^a calculator$"""){ () =>
    calculator = (x : Int, y : Int) => { x + y }
  }

  When("""^I add '(\d+)' and '(\d+)'$"""){ (x:Int, y:Int) =>
    result = calculator(x, y)
  }

  Then("""^the result is '(\d+)'$"""){ (expected:Int) =>
    result shouldEqual expected
  }
}
