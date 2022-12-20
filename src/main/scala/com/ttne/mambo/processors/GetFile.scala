package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class GetFile(spark: SparkSession, config: Config) extends BaseProcessor(spark, config){
  val outputName: String = config.getString("outputName")
  var path: String = if(config.hasPath("path")) {
    config.getString("path")
  } else "none"

  val format: String = if(config.hasPath("format")) {
    config.getString("format")
  } else "none"

  if(path.startsWith("http")){
    fileDownloader(path, "/tmp/" + path.split("/").last)
    path = "/tmp/" + path.split("/").last
  }

  val header: String = if (config.hasPath("header")) {
    config.getString("header")
  } else
    "false"

  val inferSchema: String = if (config.hasPath("inferSchema")) {
    config.getString("inferSchema")
  } else
    "false"

  val delimiter: String = if (config.hasPath("delimiter")) {
    config.getString("delimiter")
  } else
    ","

  override def run(): Unit = {
    logger.info("executing com.ttne.mambo.processors.GetFile")

    format match {
      case "csv" =>
        logger.info("Executing com.ttne.mambo.processors.GetFile - CSV")

        setDataFrame(spark
          .read
          .option("header", header)
          .option("inferSchema", inferSchema)
          .option("delimiter", delimiter)
          .format("csv")
          .load(path), outputName)
    case "xls" =>
      logger.info("Executing com.ttne.mambo.processors.GetFile - XLS")

      val sheetName = config.getString("sheetName")
      val useHeader = config.getString("useHeader")
      val treatEmptyValuesAsNulls = if(config.hasPath("treatEmptyValuesAsNulls")){
        config.getString("treatEmptyValuesAsNulls")
      } else "false"


      val addColorColumns = if(config.hasPath("addColorColumns")) {
        config.getString("addColorColumns")
      } else
        "false"

      val startColumn = if(config.hasPath("startColumn")) {
        config.getInt("startColumn")
      } else
        0

      val endColumn = if(config.hasPath("endColumn")) {
        config.getInt("endColumn")
      } else
        Int.MaxValue

      val timestampFormat = if(config.hasPath("timestampFormat")){
        config.getString("timestampFormat")
      } else "MM-dd-yyyy HH:mm:ss"

      val maxRowsInMemory = if(config.hasPath("maxRowsInMemory")) {
        config.getInt("maxRowsInMemory")
      } else
        20

      val excerptSize = if(config.hasPath("excerptSize")) {
        config.getInt("excerptSize")
      } else
        10

      setDataFrame(spark
        .read
        .format("com.crealytics.spark.excel")
        .option("sheetName", sheetName)
        .option("useHeader", useHeader)
        .option("treatEmptyValuesAsNulls", treatEmptyValuesAsNulls)
        .option("inferSchema", inferSchema)
        .option("addColorColumns", addColorColumns)
        .option("startColumn", startColumn)
        .option("endColumn", endColumn)
        .option("timestampFormat", timestampFormat)
        .option("maxRowsInMemory", maxRowsInMemory)
        .option("excerptSize", excerptSize)
        .load(path), outputName)
      case _ =>
        logger.info("Executing com.ttne.mambo.processors.GetFile - %s".format(format))

        setDataFrame(spark
          .read
          .option("header", header)
          .option("inferSchema", inferSchema)
          .option("delimiter", delimiter)
          .format(format)
          .load(path), outputName)

    }
  }

  import java.io.File
  import java.net.URL
  import sys.process._

  def fileDownloader(url: String, filename: String): Unit = {
    new URL(url) #> new File(filename) !!
  }
}
