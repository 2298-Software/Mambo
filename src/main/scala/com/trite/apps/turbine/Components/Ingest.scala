package com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Ingest(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val path: String = config.getString("path")
  val format: String = if(config.hasPath("format")) {
    config.getString("format")
  } else "none"

  val outputName: String = config.getString("outputName")

  def importFile(): Unit = {
    logger.info("executing importFile")
    setDataFrameWithOutput(spark.read.format(format).load(path), outputName)
  }

  def importCsvFile(): Unit = {
    logger.info("executing importCsvFile")
    val header = if(config.hasPath("header")) {
      config.getString("header")
    } else
      "false"

    val inferSchema = if(config.hasPath("inferSchema")) {
      config.getString("inferSchema")
    } else
      "false"

    val delimiter = if(config.hasPath("delimiter")) {
      config.getString("delimiter")
    } else
      ","

    setDataFrameWithOutput(spark
      .read
      .option("header", header)
      .option("inferSchema", inferSchema)
      .option("delimiter", delimiter)
      .format("csv")
      .load(path), outputName)
  }

  def importXlsFile(): Unit = {
    logger.info("executing importXlsFile")

    val sheetName = config.getString("sheetName")
    val useHeader = config.getString("useHeader")
    val treatEmptyValuesAsNulls = if(config.hasPath("treatEmptyValuesAsNulls")){
      config.getString("treatEmptyValuesAsNulls")
    } else "false"

    val inferSchema = if(config.hasPath("inferSchema")) {
      config.getString("inferSchema")
    } else
      "false"

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

    setDataFrameWithOutput(spark
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
  }
}
