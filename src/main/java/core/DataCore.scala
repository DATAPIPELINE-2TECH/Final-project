package core

import org.apache.spark.sql.SparkSession
import java.io.File

object DataCore {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Dataset_Cleansing")
      //.enableHiveSupport()
      .config("spark.master","local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("FATAL")
  //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  //spark.conf.set("spark.sql.hive.convertMetastoreOrc", "false")
}