package io

import org.apache.spark.sql.DataFrame
import tool.{Settings,DataSaving}

class DataSaver {
  def saveDataFrame(df: DataFrame, dbOutputConf: String, outputTableName: String): Unit = DataSaving.saveTable(df)(dbOutputConf+"."+outputTableName)
}
