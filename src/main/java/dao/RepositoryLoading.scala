package dao

import core.HiveIO
import featuresengineering.StartDataProcessing
import featuresengineering.DataLitterals._
import org.apache.spark.sql.{Column, DataFrame}



import tool.configTable

class RepositoryLoading(dao: HiveIO, path: String="") {

  //---Local CSV I/O LOADING   /** For Loading local CSVs (Unit-testing purpose) */

  var rawDf : DataFrame = dao._spark.read.option("header","true").option("inferschema","true").option("delimiter",",").csv(path)
  val renamedDf : DataFrame = rawDf/*.columns.foldLeft(rawDf)(
    (myDF, col) => myDF.withColumnRenamed(col, col.split('.')(1))
  )*/
  val localTablePack = Map(
    RAW_LIT -> renamedDf
  )


  def processlocalCleaning(processingProfile: String): (DataFrame) = {
    StartDataProcessing.getDatasetEngineering(localTablePack, processingProfile)
  }


}
