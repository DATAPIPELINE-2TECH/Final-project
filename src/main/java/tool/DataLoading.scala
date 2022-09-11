package tool

import core.DataCore
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import tool.FeatureEngineeringToolkit._


object DataLoading {

  def loadTableLastData(location: String, separator: String = "|"): DataFrame = {
    if (Settings.isDev) {
      DataCore.spark.read.format("csv")
        .option("sep", separator)
        .option("inferSchema", "false")
        .option("header", "true")
        .load(location)
    } else {
      DataCore.spark.sql("SELECT * FROM " + location + " WHERE time = (SELECT MAX(time) FROM "+location+")")
    }
  }
  def loadTableLastDataQuotes(location: String, separator: String = "|"): DataFrame = {
    if (Settings.isDev) {
      DataCore.spark.read.format("csv")
        .option("sep", separator)
        .option("inferSchema", "false")
        .option("header", "true")
        .load(location)
    } else {
      DataCore.spark.sql("SELECT * FROM " + location+ " WHERE `time` = (SELECT MAX(`time`) FROM "+location+")")
    }
  }

  def loadTable(location: String, separator: String = "|"): DataFrame = {
    if (Settings.isDev) {
      DataCore.spark.read.format("csv")
        .option("sep", separator)
        .option("inferSchema", "false")
        .option("header", "true")
        .load(location)
    } else {
      DataCore.spark.sql("SELECT * FROM " + location)
    }
  }

  def loadLiveData(partitionCol: Column)(df: DataFrame): DataFrame = {
    df.withColumn("rank", rank.over(
      Window
        .partitionBy(partitionCol)
        .orderBy(desc("time"))))
      .where(col("rank") === 1)
      .drop("rank")
  }

}
