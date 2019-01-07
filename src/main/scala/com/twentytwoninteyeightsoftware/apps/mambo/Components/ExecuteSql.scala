package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExecuteSql(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val sql: String = config.getString("sql")
  val outputName: String = config.getString("outputName")

  def run(): Unit = {
    logger.info("running executeSql")
    setDataFrameWithOutput(spark.sql(sql), outputName)
  }
}
