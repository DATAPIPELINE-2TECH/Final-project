package repository

import core.HiveIO
import org.apache.spark.sql.DataFrame


object RepoCleaning {
  def saveToHDFS(data: DataFrame, dirname: String): Unit = {
    data
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "|").
      csv("/tmp/" + dirname + "/")
  }

  def saveToHive(data: DataFrame, db: String, tableName: String, dao: HiveIO): Unit = {
    data.write.format("orc").saveAsTable(s"""$db.$tableName""")
  }
}

