package dao

import java.io.File

import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class WinHadoopEnabledTestBase extends FlatSpec with Matchers with MockFactory{

  def isWindows = System.getProperty("os.name").toLowerCase.startsWith("win")

  def getHiveFile(path: String): String = {
    new File(Thread.currentThread().getContextClassLoader.getResource(path).getFile)
      .getAbsolutePath.replace("\\","/")
  }
  if(isWindows) {
    /*
    Create the following directory structure: "C:\hadoop_home\bin"
    Download the following file: http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe
    Put this file into the "bin" directory of "C:\hadoop_home"
     */
    System.setProperty("hadoop.home.dir", "C:/hadoop_home")
  }

  /*
    Create the following directory structure: "C:\hadoop_home\tmp"
     */
  System.setProperty("java.io.tmpdir", "C:/hadoop_home/tmp")
}
