package tool

import org.apache.spark.sql.DataFrame

object DataSaving {

  def saveTable(df: DataFrame)(location: String, separator: String = "|"): Unit = {
    if (Settings.isDev) {
      df.write.option("sep", separator).csv(location)
    } else {
      df.write.mode("overwrite").format("orc").option("compression", "zlib")
        .saveAsTable(location)
    }
  }

}
