package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class PutFile(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val path: String = config.getString("path")
  val format: String = config.getString("format")
  val query: String = config.getString("query")
  val parallelWrites: Int = if(config.hasPath("parallelWrites")) {
    config.getInt("parallelWrites")
  } else {1}

  val saveMode: String = if(config.hasPath("saveMode")) {
    config.getString("saveMode")
  } else {
    "error"
  }

  def run(): Boolean = {
    logger.info("executing saveFile")
    val df: DataFrame = spark.sql(query)
    df.coalesce(parallelWrites).write.mode(saveMode).format(format).save(path)
    true
  }
}
