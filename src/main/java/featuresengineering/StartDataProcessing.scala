package featuresengineering

import featuresengineering.DataLitterals._
import featuresengineering.DataMunging._
import org.apache.spark.sql.DataFrame
object StartDataProcessing {



  def getDE(dataPack: Map[String, DataFrame]): Map[String, DataFrame] = {

    /** Importing Data */

    dataPack.keys.map(key => key ->{
      key match {
        case "analytics_df_cities"    => getDEBrazilCovidCities(dataPack(key))
        case "analytics_df"    => getDEBrazilCovid(dataPack(key))
      }

    }).toMap

  }



  def getDatasetEngineering(dataPack: Map[String, DataFrame], processingProfile: String): DataFrame = {

    /** Importing Data */
    val dfRawData = dataPack.apply(RAW_LIT)

    processingProfile match {
      case "P001"    => getDEBrazilCovidCities(dfRawData)
      case "P002"    => getDEBrazilCovid(dfRawData)
    }
  }
}
