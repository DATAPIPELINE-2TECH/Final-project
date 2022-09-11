package tool


import java.sql.Timestamp

import core.DataCore.spark
import core.DataCore.spark.implicits._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.joda.time.DateTime


object FeatureEngineeringToolkit {

  def saveDF(dataFrame: DataFrame) : Unit = {
    dataFrame.write.option("header",true)
      //.option("delimiter",";")
      .csv("data/")
  }

  def refDF(dataFrame: DataFrame) : DataFrame = {
    dataFrame.select(dataFrame("state"), dataFrame("region"))
      .dropDuplicates()
      .withColumnRenamed("state", "state_ref")
  }

  def structDF(dataFrame1: DataFrame, dataFrame2: DataFrame) : DataFrame = {
    dataFrame1.join(dataFrame2, dataFrame1("state") === dataFrame2("state_ref"), "left")
      .select("date","region","state","cases","deaths")
      .orderBy("date","region", "state")
      .dropDuplicates()
      // Now, we group df1 (cases, deaths) by state
      .groupBy("date", "region", "state")
      .sum()
      .orderBy("date", "region", "state")
      .withColumnRenamed("sum(cases)", "cases")
      .withColumnRenamed("sum(deaths)", "deaths")
  }


}
