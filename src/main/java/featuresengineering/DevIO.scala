package featuresengineering

import core.DataCore
import core.DataCore.spark
import org.apache.spark.sql.DataFrame
import tool.configTable

/**
 * IO class for local development purposes
 */
object DevIO {

  /** For Loading a csv (Unit-testing purpose) */
  def loadCsvFalse(filename:String, separator:String="|"): DataFrame = {
    DataCore.spark.read.format("csv")
      .option("sep", separator)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("timestampFormat", "yyyy-MM-dd")
      .load(filename)
  }

  def saveCsvDev(df:DataFrame): Unit = df.write.option("sep","|").option("header", "true").option("inferSchema", "false").mode("overwrite").format("csv").save("C:/out")

}
