package io

import org.apache.spark.sql.DataFrame
import tool.{DataLoading, Settings}

class DataLoader {
  def getAnalyticsDF:DataFrame = DataLoading.loadTableLastDataQuotes(Settings.input.analytics_df)
}
