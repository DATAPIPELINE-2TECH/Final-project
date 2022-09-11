package data

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import dao.JobTestBase
import featuresengineering.DataLitterals._
import featuresengineering.DevIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.AssertTrue
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.junit.Assert.assertEquals
import tool.configTable
import org.apache.spark.internal.{Logging, config}


@RunWith(classOf[JUnitRunner])
class FeaturesEngineeringTest extends JobTestBase {

  "testFeaturesEngineering_3m" should "work" in {

    /**
    val tablePack: Map[String, DataFrame] = Map(
      RAW_LIT -> DevIO.loadCsvFalse("src/test/resources/feature_engineering_cs/raw_scala.csv")
    )

    val dfOutput = DevIO.loadCsvFalse("src/test/resources/feature_engineering_cs/output_3m.csv").na.fill("")

    **/


    //val result = getDatasetEngineered(tablePack).na.fill("")

    //assertEquals(dfOutput.columns.length, result.columns.length)
    //assertEquals(dfOutput.columns.sorted.mkString("|"), result.columns.sorted.mkString("|"))
    //assertEquals(dfOutput.count(), result.count())
    //assertEquals(dfOutput.collect().map(r => r.mkString("|")).mkString("@"),
      //result.collect().map(r => r.mkString("|")).mkString("@"))
  }
}
