package featuresengineering

import core.DataCore.spark
import featuresengineering.DataLitterals._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}
import tool.DataLoading.loadLiveData
import tool.FeatureEngineeringToolkit._
//import DevIO.getLoadRawScala
import org.apache.spark.sql.functions._

object DataMunging {

  /**
   * Get DF of brazil_covid19_cities.csv
   */
  def getDEBrazilCovid(dfRawData : DataFrame) : DataFrame = {
    dfRawData
      //.transform()
  }

  /**
   * Get DF of brazil_covid19.csv
   */
  def getDEBrazilCovidCities(dfRawData: DataFrame): DataFrame = {
    dfRawData
      //.transform()
  }

}


