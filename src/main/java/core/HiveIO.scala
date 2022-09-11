package core

import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}


object HiveIO {
  @transient lazy val log = LogManager.getRootLogger
  val LeftOuter = "left_outer"
  val FullOuter = "full_outer"
}

class HiveIO(spark: SparkSession, db:String){

  val _spark: SparkSession = spark

  import _spark.sql

  def liveData(table: String, gByColumn: String):DataFrame =  {
    liveData(table, Seq(gByColumn))
  }
  def liveData(table: String, gByColumn: String, cols: String): DataFrame =  {
    var c = cols + ", time"
    liveData(table, Seq(gByColumn), c)
  }

  def liveData(table: String, gByColumns: Seq[String]):DataFrame =  {
    liveData(table, gByColumns, "*")
  }

  def liveData(table: String, gByColumns: Seq[String], cols: String ):DataFrame =  {
    val dataMaxDate = sql(s"select MAX(time) as time, ${gByColumns.mkString(", ")} from $db.$table GROUP BY ${gByColumns.mkString(", ")}")
    sql(s"select $cols from $db.$table ")
      .join(dataMaxDate,  gByColumns :+ "time")
  }

  def liveData(query : String): DataFrame ={
    sql(s"$query")
  }

  def liveDataByDate(table: String, gByColumns: String, cols: String, date: String): DataFrame = {
    var c = cols + ", time"
    var gByCols = Seq(gByColumns)
    liveDataByDate(table, gByCols, c, date)
  }

  def liveDataByDate(table: String, gByColumns: Seq[String], cols: String, date: String): DataFrame = {
    val dataByDate = sql(s"select $cols from $db.$table where to_date(time) <= $date")
    var dataMaxDate : DataFrame = null

    if(gByColumns.length > 1) {
      dataMaxDate = dataByDate
        .selectExpr(s"${gByColumns.head}", s"${gByColumns.tail: _*}", "time")
        .groupBy(s"${gByColumns.head}", s"${gByColumns.tail: _*}")
        .agg(max("time") as "time")
    } else {
      dataMaxDate = dataByDate
        .selectExpr(s"${gByColumns.head}", "time")
        .groupBy(s"${gByColumns.head}")
        .agg(max("time") as "time")
    }

    dataByDate
      .join(dataMaxDate,  gByColumns :+ "time")
  }

  def lastCodification(cols: String, typefille: String):DataFrame =  {
    sql(
      s"""select $cols
         | from $db.orc_codification
         | where time = (select Max(time) from $db.orc_codification)
         | and typefille = "$typefille" """.stripMargin)
  }
  def lastData(table : String, cols: String ): DataFrame ={
    sql(s"select $cols from $db.$table where time = (select Max(time) from $db.$table)")
  }

  def lastData(table : String, cols: String, date: String ): DataFrame ={
    sql(s"select $cols from $db.$table where time = (select Max(time) from $db.$table where to_date(time) <= $date)")
  }

  def ALLData(table : String, cols: String ): DataFrame ={
    sql(s"select $cols from $db.$table")
  }

  def ALLData(table : String, cols: String, date: String): DataFrame ={
    sql(s"select $cols from $db.$table where to_date(time) <= $date")
  }

  def all[T: Encoder](table: String):Dataset[T] ={
    sql(s"select * from $db.$table").as[T]
  }

  def dataBetweenFields(table: String,cols :String, field: String, start: String , end: String): DataFrame ={
    sql(
      s"""select $cols
         |from $db.$table
         |where $field between '$start' and '$end'""".stripMargin)
  }

  def joinData(firstTable:String, secondTable:String, colsFirstTable:String , colsSecTable:String, listBilan: String): DataFrame ={

    val transaction = spark.sql(s"""select $colsFirstTable from $db.$firstTable""".stripMargin)//.withColumnRenamed("n_cpte_benef", "numero")
    val compteHost = spark.sql(s"""select $colsSecTable from $db.$secondTable where bilan IN $listBilan""".stripMargin)
    val joinedData = transaction.join(compteHost, col("n_cpte_benef") === col("numero"), "left_outer")
    joinedData
  }

  def getMaxDate(table: String): String = {
    val maxDate = sql(s"select cast(max(to_date(time)) as string) as maxtime from $db.$table")
    val s = maxDate.select("maxtime").collect().map(_(0).asInstanceOf[String]).toList
    s.head
  }

  def getMaxTimeStamp(table: String): Timestamp = {
    val maxDate = sql(s"select cast(max(to_date(time)) as string) as maxtime from $db.$table")
    val s = maxDate.select("maxtime").collect().map(_(0).asInstanceOf[Timestamp]).toList
    s.head
  }

  def getDBName: String ={
    db
  }
}