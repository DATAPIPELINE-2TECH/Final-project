package mr

import core.HiveIO
import dao.RepositoryLoading
import featuresengineering.DataLitterals.{paths, _}
import tool.FeatureEngineeringToolkit._
import featuresengineering.StartDataProcessing
import org.apache.spark.sql.DataFrame
import tool.configTable
import org.apache.spark.sql.functions._
import tool.DataLoading._

object MainRun {
  def main(args: Array[String]): Unit = {


    val spark = core.DataCore.spark
    val dao = new HiveIO(spark, configTable.dbName)

    // Brazil covid cities
    val path1 : String = "P001"
    val loadedRepo = new RepositoryLoading(dao,paths(path1))
    val df1 = loadedRepo.processlocalCleaning(path1)

    // Brazil covid
    val path2 : String = "P002"
    val loadedRepo2 = new RepositoryLoading(dao,paths(path2))
    val df2 = loadedRepo2.processlocalCleaning(path2)


    println("-------------------------begin-------------------------")


    val df_ref : DataFrame = refDF(df2)

    // We can easily save the ref table by removing the // sign in the below line
    // saveDF(df_ref)

    val df1_wstruct : DataFrame = structDF(df1, df_ref)

    // We then save our result
    // saveDF(df1_wstruct)

    df1_wstruct.show(100)


    println("-------------------------end-------------------------")
  }
}


