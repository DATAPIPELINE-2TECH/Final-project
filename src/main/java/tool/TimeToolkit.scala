package tool

import java.sql.{Date, Timestamp}
import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


object TimeToolkit {

  val nowDate = new Date(getNewNowCal.getTimeInMillis)
  val nowTs = new Timestamp(getNewNowCal.getTimeInMillis)
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val timeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val compactTimeFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss")

  def getNewNowCal = Calendar.getInstance()

  final val MILLISECONDS_IN_YEAR: Long = 365 * 24 * 60 * 60 * 1000L
  final val MILLISECONDS_IN_DAY: Long =  24 * 60 * 60 * 1000L

  def dateToCal(date:Date) = {
    val cal = getNewNowCal
    cal.setTime(date)
    cal
  }

  val millisToDate = udf {(u:Long) => new Timestamp(u) }

  val GenerateYearMonthDayUDF: UserDefinedFunction = udf(ymd(_: Int, _: Int, _: Int))
  /**
   * Get a gregorian date based on year, month and day :)
   * @param month is 1-based  (7 = July) hence the month-1 in implementation
   *              for GregorianCalendar
   * @return
   */
  def ymd(year: Int, month: Int, day: Int) = {
    new Date(new GregorianCalendar(year, month-1, day).getTimeInMillis)
  }

  /**
   * Classic date constructor
   * @param month STARTING WITH 1
   */
  def ymdhm(year: Int, month: Int, day: Int, hour: Int=0, minute: Int=0) = {
    new Timestamp(new GregorianCalendar(year, month-1, day, hour, minute).getTimeInMillis)
  }

  /**
   *
   * @param dateColName Date column name to consider
   * @return a Date representation of "2016-06-01" given any "2016-06-dd" where dd is any day
   */
  def to_first_dom_date(dateColName: String): Column = {
    to_date(
      unix_timestamp(
        concat_ws("-", year(col(dateColName)), month(col(dateColName)), lit(1)), "yyyy-MM-dd")
        .cast(TimestampType)
    )
  }

  def getLastYear(dt: String) = {
    val cal = Calendar.getInstance()
    cal.setTime(dateFormat.parse(dt))
    cal.add(Calendar.YEAR, -1)
    dateFormat.format(cal.getTime)
  }

  def getLastSaturday(dt: String) = {
    val cal = Calendar.getInstance()
    cal.setTime(dateFormat.parse(dt))
    cal.add(Calendar.DATE,  - (cal.get(Calendar.DAY_OF_WEEK) + Calendar.SATURDAY) % 7 )
    dateFormat.format(cal.getTime)
  }

  def getNextFriday(dt: String) = {
    val cal = Calendar.getInstance()
    cal.setTime(dateFormat.parse(dt))
    cal.add(Calendar.DATE, 6 - (cal.get(Calendar.DAY_OF_WEEK) + Calendar.SATURDAY) % 7)
    dateFormat.format(cal.getTime)
  }

  def getCurrentWeek(dt: String) = {
    getLastSaturday(dt) + "/" + getNextFriday(dt)
  }

  def timeme(lib:String)(block: => Unit): String = {
    val t0 = System.currentTimeMillis()
    block
    val ts = (System.currentTimeMillis() - t0)/1000
    "%s in %d min %d sec\n".format(lib, ts/60, ts-60*(ts/60))
  }


}
