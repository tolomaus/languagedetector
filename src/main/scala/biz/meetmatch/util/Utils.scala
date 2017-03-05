package biz.meetmatch.util

import java.io.{File, InputStream}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import biz.meetmatch.logging.BusinessSparkListener
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.rogach.scallop.Scallop
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.experimental.macros

object Utils {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private val cf: Config = ConfigFactory.load("local").withFallback(ConfigFactory.load())

  def getFiltersFromCLI(args: Array[String]): Scallop = {
    Scallop(args)
      .opt[String]("message", 'm', required = false, default = () => Some("no message"))
      .opt[String]("file", 'f', required = false)
      .verify
  }

  def getStreamingFiltersFromCLI(args: Array[String]): Scallop = {
    Scallop(args)
      .verify
  }

  def getConfig(path: String): String = {
    cf.getString(path)
  }

  def getDefaultNumPartitions(implicit sparkSession: SparkSession): Int = {
    sparkSession.sparkContext.defaultParallelism * cf.getInt("app.numPartitionsPerCpu")
  }

  def createSparkSession(appName: String = "languagedetector", streaming: Boolean = false, sparkPort: Int = 4040): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(cf.getString("spark.master"))
      .config("spark.driver.host", "localhost") // without this setting spark fails when run on mac without an internet connection
      .config("spark.driver.memory", cf.getString("spark.driver.memory"))
      .config("spark.executor.memory", cf.getString("spark.executor.memory"))
      .config("spark.eventLog.enabled", "true")
      .config("spark.sql.shuffle.partitions", "16") //default is 200
      .config("spark.ui.port", sparkPort)
      .getOrCreate()

    if (!streaming)
      sparkSession.sparkContext.addSparkListener(new BusinessSparkListener)

    sparkSession
  }

  def createSparkStreamingSession(implicit sparkSession: SparkSession): StreamingContext = {
    new StreamingContext(sparkSession.sparkContext, Seconds(1))
  }

  def getParquetRoot: String = {
    s"${Utils.getConfig("languagedetector.dir")}/data/parquet"
  }

  def loadParquetFile(path: String, setJobDescription: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.sparkContext.setJobDescription(s"Load parquet file - $path (meta data only)")
    sparkSession.read.load(s"$getParquetRoot/$path")
  }

  def saveAsParquetFile[T](ds: Dataset[T], path: String): Unit = {
    ds.write.mode(SaveMode.Overwrite).save(s"$getParquetRoot/$path")
  }

  def moveParquetFile(oldPath: String, newPath: String): Unit = {
    val oldPathFile = new File(s"$getParquetRoot/$oldPath")
    val result = oldPathFile.renameTo(new File(s"$getParquetRoot/$newPath"))
    if (!result)
      throw new Exception(s"Unable to rename parquet file from $oldPath to $newPath")
  }

  def existsParquetFile(path: String): Boolean = {
    new File(s"$getParquetRoot/$path").exists
  }

  def deleteParquetFile(path: String): Unit = {
    FileUtils.deleteDirectory(new File(s"$getParquetRoot/$path"))
  }

  def getTextFileRoot: String = {
    s"${Utils.getConfig("languagedetector.dir")}/data/text"
  }

  def saveAsTextFile[T](ds: Dataset[String], path: String): Unit = {
    val absolutePath = new File(s"$getTextFileRoot/$path")
    if (absolutePath.exists) FileUtils.deleteDirectory(absolutePath)

    ds.write.text(s"$getTextFileRoot/$path")
  }

  def loadTextFileAbs(path: String)(implicit sparkSession: SparkSession): Dataset[String] = {
    sparkSession.read.textFile(path)
  }

  def loadTextFile(path: String)(implicit sparkSession: SparkSession): Dataset[String] = {
    import sparkSession.implicits._
    sparkSession.sparkContext.textFile(s"$getTextFileRoot/$path").toDS
  }

  def loadWholeTextFileAbs(path: String)(implicit sparkSession: SparkSession): Dataset[(String, String)] = {
    import sparkSession.implicits._
    sparkSession.sparkContext.wholeTextFiles(path).toDS
  }

  def loadWholeTextFile(path: String)(implicit sparkSession: SparkSession): Dataset[(String, String)] = {
    import sparkSession.implicits._
    sparkSession.sparkContext.wholeTextFiles(s"$getTextFileRoot/$path").toDS
  }

  def changeNumPartitionsOfParquetFile(path: String, numPartitions: Int)(implicit sparkSession: SparkSession): Boolean = {
    val parquetFile = loadParquetFile(path).repartition(numPartitions)

    val pathTmp = s"$path.tmp"
    val pathTmpFile = new File(pathTmp)
    saveAsParquetFile(parquetFile, pathTmp)

    val pathFile = new File(path)
    FileUtils.deleteDirectory(pathFile)

    pathTmpFile.renameTo(pathFile)
  }

  def toListOption[A](to: TraversableOnce[A]): Option[List[A]] = {
    to.toList match {
      case x :: xs => Some(x :: xs)
      case Nil => None
    }
  }

  def setBusLogFileNamePropertyIfEmpty(): Unit = {
    if(System.getProperty("businessLogFileName") == null) {
      val jarDir = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParent
      val date = new SimpleDateFormat("yyyyMMddHHmm").format(Calendar.getInstance().getTime)
      val busLogFileName = s"$jarDir/logs/businessLog_$date.log"

      System.setProperty("businessLogFileName", busLogFileName)
    }
  }

  def getTimestamp: String = {
    new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime)
  }

  def getDuration(startDate: Date, stopDate: Date): FiniteDuration = {
    Duration.fromNanos(TimeUnit.NANOSECONDS.convert(stopDate.getTime, TimeUnit.MILLISECONDS) - TimeUnit.NANOSECONDS.convert(startDate.getTime, TimeUnit.MILLISECONDS))
  }

  def getDuration(seconds: Int): FiniteDuration = {
    Duration.fromNanos(TimeUnit.NANOSECONDS.convert(seconds, TimeUnit.SECONDS))
  }

  def formatDuration(duration: FiniteDuration): String = {
    val elapsedInHours = duration.toHours
    val elapsedInMinutes = duration.toMinutes % 60
    val elapsedInSeconds = duration.toSeconds % 60
    "%01dh%02dm%02ds".format(elapsedInHours, elapsedInMinutes, elapsedInSeconds)
  }

  def parseDuration(duration: String): Int = {
    val values = duration.split("h|m|s")
    if (values.length == 3)
      values(0).toInt * 60 * 60 + values(1).toInt * 60 + values(2).toInt
    else
      0
  }

  def loadWordsFromResource(resourceName: String): List[String] = {
    val stream: InputStream = getClass.getResourceAsStream("/" + resourceName)
    scala.io.Source.fromInputStream(stream).getLines().toList
  }

  def roundDouble(double: Double, precision: Int = 3): Double = {
    val dimension = Math.pow(10, precision)
    Math.round(double * dimension) / dimension
  }
}

