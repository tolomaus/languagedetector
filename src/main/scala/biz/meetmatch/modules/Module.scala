package biz.meetmatch.modules

import java.sql.Timestamp

import biz.meetmatch.decorators.{WithCalcLogging, WithSparkSession}
import biz.meetmatch.logging.BusinessLogger
import biz.meetmatch.util.Utils
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.Scallop
import org.slf4j.{Logger, LoggerFactory}

trait Module {
  protected val logger: Logger = {
    Utils.setBusLogFileNamePropertyIfEmpty()
    LoggerFactory.getLogger(this.getClass)
  }

  protected implicit val module: Class[_ <: Module] = this.getClass

  def main(args: Array[String]): Unit = {
    val scallopts = Utils.getFiltersFromCLI(args)
    logger.info(scallopts.summary)

    WithSparkSession() { implicit sparkSession =>
      WithCalcLogging(scallopts, sparkSession) {
        execute(scallopts)
      }
    }
  }

  def execute(scallopts: Scallop)(implicit sparkSession: SparkSession): Unit
}

abstract class StreamingModule(sparkPort: Int) {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  protected implicit val module: Class[_ <: StreamingModule] = this.getClass

  def main(args: Array[String]): Unit = {
    val scallopts = Utils.getStreamingFiltersFromCLI(args)
    logger.info(scallopts.summary)

    WithSparkSession(streaming = true, sparkPort = sparkPort) { implicit sparkSession =>
      execute
    }
  }

  def execute(implicit sparkSession: SparkSession): Unit = {}
}

trait ParquetExtensions[T] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val parquetFile: String

  def saveResultsToParquet(ds: Dataset[T])(implicit module: Class[_] = this.getClass, sparkSession: SparkSession): Unit = {
    saveResultsToParquetAsDF(ds.toDF)
  }

  def saveResultsToParquetAsDF(df: DataFrame)(implicit module: Class[_] = this.getClass, sparkSession: SparkSession): Unit = {
    val parquetFileTemp = parquetFile + ".inprogress"

    if (Utils.existsParquetFile(parquetFileTemp))
      Utils.deleteParquetFile(parquetFileTemp)

    logger.debug(s"Saving $parquetFile to a temporary parquet file $parquetFileTemp...")
    try {
      Utils.saveAsParquetFile(df, parquetFileTemp)
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred while saving the temporary parquet file, leaving the original parquet file $parquetFile untouched.")
        throw e
    }

    sparkSession.sparkContext.setJobGroup(module.getClass.getName + ".save", module.getClass.getName + ".save")

    val countBefore =
      if (Utils.existsParquetFile(parquetFile)) {
        val parquetFileBefore = loadResultsFromParquetAsDF(module, sparkSession)
        sparkSession.sparkContext.setJobDescription(s"Count parquet file before - $parquetFile")
        parquetFileBefore.count
      }
      else
        0

    val parquetFileAfter = Utils.loadParquetFile(parquetFileTemp)
    sparkSession.sparkContext.setJobDescription(s"Count parquet file after - $parquetFile")
    val countAfter = parquetFileAfter.count
    new BusinessLogger(module.getName).dataParquetWritten(parquetFile, countBefore, countAfter)
    logger.info(s"Stored $countAfter $parquetFile, diff is ${countAfter - countBefore}")

    logger.debug(s"Moving the temporary parquet file $parquetFileTemp to its final destination...")
    if (Utils.existsParquetFile(parquetFile))
      Utils.deleteParquetFile(parquetFile)

    Utils.moveParquetFile(parquetFileTemp, parquetFile)
  }

  def loadResultsFromParquetAsDFO(implicit module: Class[_] = this.getClass, sparkSession: SparkSession): Option[DataFrame] = {
    if (Utils.existsParquetFile(parquetFile))
      Some(loadResultsFromParquetAsDF(module, sparkSession))
    else
      None
  }

  def loadResultsFromParquetAsDF(implicit module: Class[_] = this.getClass, sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading parquet from $parquetFile...")
    new BusinessLogger(module.getName).dataParquetRead(parquetFile)
    Utils.loadParquetFile(parquetFile) // dont call loadResultsFromParquetAsDF to avoid a stack overflow when it is overridden in the subclass
  }

  def loadResultsFromParquetAsDF(implicit sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading parquet from $parquetFile...")
    Utils.loadParquetFile(parquetFile)
  }

  def getLastModifiedFromParquetO(implicit module: Class[_] = this.getClass, sparkSession: SparkSession): Option[Timestamp] = {
    loadResultsFromParquetAsDFO
      .flatMap { dataframe =>
        if (dataframe.columns.contains("lastModified"))
          dataframe
            .select("lastModified")
            .agg(max("lastModified"))
            .collect
            .headOption
            .map(_.getTimestamp(0))
        else
          None
      }
  }
}
