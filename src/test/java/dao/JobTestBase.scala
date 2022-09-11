package dao


import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


import scala.util.control.Breaks._


@RunWith(classOf[JUnitRunner])
class JobTestBase extends WinHadoopEnabledTestBase with Logging {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.debug.maxToStringFields", 500)
    .master("local")
    .appName("test")
    .getOrCreate()


  val dataTypesMapper = Map(
    "String" -> DataTypes.StringType,
    "Integer" -> DataTypes.IntegerType,
    "Double" -> DataTypes.DoubleType,
    "BigDecimal" -> DataTypes.DoubleType
  )

  def emptyDF(schemaString: String): DataFrame = {
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, DataTypes.StringType, nullable = true))
    val schema = StructType(fields)
    val ov = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    ov
  }

  def createDataframeFromRow(data: Row, schemaString: String): DataFrame = {
    var dataTypes = Array[String]()

    data.toSeq.foreach(field => {
      if (field == null) // default nulls to string datatype
      dataTypes = dataTypes :+ "String"
      else
      dataTypes = dataTypes :+ field.getClass.toString.replace("class java.lang.", "")
    })
    //data.toSeq.foreach(field => dataTypes = dataTypes :+ field.getClass.toString.replace("class java.lang.", ""))

    val schemaColumns = schemaString.split(" ")
    val fields = (schemaColumns zip dataTypes).map(field => StructField(field._1, dataTypesMapper(field._2), nullable = true))
    val schema = StructType(fields)
    val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(Seq(data)), schema)

    dataFrame
  }

  def sortDataframeCols(dataframe: DataFrame): DataFrame = {
    val cols = dataframe.columns.sorted
    try
      dataframe.select(cols.map(x => col(x)): _*)
    catch {
      case e: AnalysisException =>
        log.error("YOUR DATAFRAME CONTAINS DUPLICATE COLUMNS. PLEASE REMOVE THEM.")
        log.error("EXCEPTION: ", e)
        dataframe
    }
  }

  def assertDataframeEquals(expected: DataFrame, actual: DataFrame) {
    val sortedExpected = sortDataframeCols(expected)
    val sortedActual = sortDataframeCols(actual)

    var areDataframesEqual = true

    if (sortedExpected.count() == sortedActual.count()) {

      try
        areDataframesEqual = sortedExpected.union(sortedActual).distinct.count == sortedExpected.intersect(sortedActual).count
      catch {
        case e: AnalysisException =>
          log.error("EXCEPTION: " + e)
          areDataframesEqual = false
      }
    } else {
      areDataframesEqual = false
      log.error("DATAFRAME ARE NOT OF EQUAL SIZE.")
    }

    assert(areDataframesEqual, true)
  }

  def assertDataframeEquals(expected: DataFrame, actual: DataFrame, debug: Boolean) {
    if (debug === false) {
      assertDataframeEquals(expected, actual)
    } else {
      val columns = expected.schema.fields.map(_.name)

      //println(columns.deep.mkString("\n"))

      var areDataframesEqual = true
      //print("DEBUG")
      //print("df_result size" + expected.count())
      //println("df_output size " + actual.count())
      //print("TOUT VA BIEN JE NE SUIS PAS FOLLE")

      //print("TOTO_EXPECTED")
      //expected.columns.foreach(print)
      //print("TOTO_ACTUAL")
      //actual.columns.foreach(print)
      //print("ALORS CA DOIT MARCHER")

      if (expected.count() == actual.count()) {
        breakable {
          try {
            val selectiveDifferences = columns.map(col => expected.select(col).except(actual.select(col)))

            //println(selectiveDifferences.deep.mkString("\n"))

            selectiveDifferences.foreach(
              diff => {

                print("LES DIFFERENCES SONT" + diff.count)
                if (diff.count > 0) {
                  areDataframesEqual = false
                  log.info("DIFFERENCES: ")
                  break
                }
              }
            )
          } catch {
            case e: AnalysisException =>
              log.error("EXCEPTION: " + e)
              areDataframesEqual = false
          }
        }
      } else {
        areDataframesEqual = false
        log.error("DATAFRAME ARE NOT OF EQUAL SIZE.")
      }

      assert(areDataframesEqual, true)
    }
  }
}
