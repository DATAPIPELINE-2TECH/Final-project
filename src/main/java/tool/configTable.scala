package tool

import org.apache.spark.internal.Logging
import com.typesafe.config
import com.typesafe.config.ConfigFactory

object configTable extends configTable
class configTable extends  Logging {

  var conf: config.Config = _
  var env: String =""
  var dbName: String  =""
  var tableNameInput: String = ""
  var tableNameOutput: String = ""
  var tableNameFct: String = ""
  var tableProcessingProfile: String = ""

  def read(args: Array[String]): Unit = {
    if(args.length >=1) {
      env = args(0)
      log.info(s"USING ENV : $env")
    }
  }

  def init(): Unit = {
    conf = ConfigFactory.load()
    conf = conf.getConfig(env)
    dbName = conf.getString("dbName")
  }

  def apply(): Unit = {
    conf = ConfigFactory.load()
    conf = conf.getConfig(env)
    tableNameInput = conf.getString("feature_engineering.tableNameInput")
    tableNameOutput = conf.getString("feature_engineering.tableNameOutput")
    tableProcessingProfile = conf.getString("feature_engineering.tableProcessingProfile")
    dbName = conf.getString("feature_engineering.dbName")
  }

  def getTable(tableName : String) : Unit = {
    conf = ConfigFactory.load()
    conf = conf.getConfig("preprocessing")
    tableNameInput = conf.getString("input."+tableName)
    tableNameOutput = conf.getString("output."+tableName)
  }
}
