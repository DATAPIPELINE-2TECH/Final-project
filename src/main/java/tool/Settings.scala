package tool

import com.typesafe.config.{Config, ConfigFactory}

object Settings {

  val conf: Config = ConfigFactory.load()

  val dbInputConf: String = conf.getString("inputs.db")
  val dbOutputConf: String = conf.getString("outputs.db")

  val isDev: Boolean = conf.getString("inputs.env") == "dev"
  val isRec: Boolean = conf.getString("inputs.env") == "rec"

  val dbInputLoc: String = dbInputConf + "."
  val dbOutputLoc: String = dbOutputConf + "."


  object input {
    // Raw data
    val analytics_df: String = dbInputLoc + conf.getString("inputs.analytics_transaction")
  }

  object output {
    // Final tables
    val analytics_transactionLocation: String = dbOutputLoc + conf.getString("outputs.analytics_transaction")
    val analytics_transactionDescribeLocation: String = dbOutputLoc + conf.getString("outputs.analytics_transaction_describe")
    val analytics_transactionCleanOutlierLocation: String = dbOutputLoc + conf.getString("outputs.analytics_transaction_clean_outlier")
    val analytics_transactionProcessingOutlierLocation: String = dbOutputLoc + conf.getString("outputs.analytics_transaction_processing_outlier")
  }
}