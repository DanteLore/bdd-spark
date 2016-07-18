import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers
import scala.collection.convert.wrapAsScala._

case class HousePriceRow(price : Int, postcode : String, houseType : String)

class SparkSteps extends ScalaDsl with EN with Matchers {

  import Spark.sqlContext.implicits._

  var result : Any = 0

  When("""^I count the words in "([^"]*)"$"""){ (input:String) =>
    result = Spark.sc.parallelize(input.split(' ')).count()
  }

  Then("""^the number of words is '(\d+)'$"""){ (expected:Long) =>
    result shouldEqual expected
  }

  Given("""^a table of house price data in a temp table "([^"]*)"$"""){ (tableName : String, table:DataTable) =>
    val data = table.asList[HousePriceRow](classOf[HousePriceRow]).toList
    Spark.sc.parallelize(data)
      .toDF()
      .registerTempTable(tableName)
  }

  When("""^I execute SQL "([^"]*)"$"""){ (sql:String) =>
    val x = Spark.sqlContext
      .sql(sql)
      .map(x => x.get(0))
      .collect()
      .foreach(x => result = x)
  }

  Then("""^the query result is '(\d+)'$"""){ (expected:Int) =>
    result shouldEqual expected
  }
}
