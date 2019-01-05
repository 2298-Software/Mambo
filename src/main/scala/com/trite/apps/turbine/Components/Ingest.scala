package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Ingest(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val path: String = config.getString("path")
  val format: String = config.getString("format")
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

    val inferSchema = if(config.hasPath("inferSChema")) {
      config.getString("inferSchema")
    } else
      "false"

    setDataFrameWithOutput(spark
      .read
      .option("header", header)
      .option("inferSchema", inferSchema)
      .format(format)
      .load(path), outputName)
  }
}
