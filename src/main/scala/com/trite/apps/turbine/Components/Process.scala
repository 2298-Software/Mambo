package com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Process(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val sql: String = config.getString("sql")
  val outputName: String = config.getString("outputName")

  def executeSql(): Unit = {
    logger.info("running executeSql")
    setDataFrameWithOutput(spark.sql(sql), outputName)
  }
}
