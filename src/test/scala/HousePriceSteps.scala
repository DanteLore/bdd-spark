import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers
import org.apache.spark.sql.functions._

import scala.collection.convert.wrapAsScala._

class HousePriceSteps extends ScalaDsl with EN with Matchers {
  import HousePrices._

  When("""^I count the rows in the table "([^"]*)"$"""){ (tableName:String) =>
    Context.result = countRows(tableName)
  }
  When("""^I calculate the average house price per year in the table "([^"]*)" and store the results in table "([^"]*)"$"""){ (input:String, output:String) =>
    justDoIt(input, output)
  }
  When("""^I calculate the average min and max price per year in the table "([^"]*)" and store the results in table "([^"]*)"$"""){ (input:String, output:String) =>
    doItWithMinMax(input, output)
  }

  When("""^I calculate the average min and max price per year in the table "([^"]*)" and postcode "([^"]*)" and store the results in table "([^"]*)"$"""){ (input:String, postcode:String, output:String) =>
    doItByPostcode(input, postcode, output)
  }
  When("""^I convert the Sales date in "([^"]*)" to a year and store in temp table "([^"]*)"$"""){ (input:String, output:String) =>
    val df = Spark.sqlContext.sql(s"select * from $input")
    val newDf = df.withColumn("SalesYear", toYear(col("SalesDate"))).drop(col("SalesDate"))
    newDf.registerTempTable(output)
  }
}
