package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by joe on 1/1/2019.
  */
class Ingest(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val path: String = config.getString("path")
  val format: String = config.getString("format")

  def importFile(): Unit = {
    logger.info("executing importFile")
    setDataFrame(spark.read.format(format).load(path))
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

    setDataFrame(spark
      .read
      .option("header", header)
      .option("inferSchema", inferSchema)
      .format(format)
      .load(path))
  }
}
