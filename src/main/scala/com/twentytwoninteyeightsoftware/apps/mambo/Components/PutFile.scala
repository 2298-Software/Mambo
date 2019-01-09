package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class PutFile(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val path: String = config.getString("path")
  val format: String = config.getString("format")
  val query: String = config.getString("query")

  val saveMode: String = if(config.hasPath("saveMode")) {
    config.getString("saveMode")
  } else {
    "error"
  }

  def run(): Unit = {
    logger.info("executing saveFile")
    val df: DataFrame = spark.sql(query)
    setDataFrame(df).write.mode(saveMode).format(format).save(path)
  }
}
