package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class PutFile(spark: SparkSession, config: Config) extends BaseProcessor(spark, config){
  val path: String = config.getString("path")
  val format: String = config.getString("format")
  val query: String = config.getString("query")
  val parallelWrites: Int = if(config.hasPath("parallelWrites")) config.getInt("parallelWrites") else 1
  val saveMode: String = if(config.hasPath("saveMode")) config.getString("saveMode") else "error"

  override def run(): Unit = {
    logger.info("executing %s".format(this.getClass.getName))
    val df: DataFrame = spark.sql(query)
    df.coalesce(parallelWrites).write.mode(saveMode).format(format).save(path)

  }
}
