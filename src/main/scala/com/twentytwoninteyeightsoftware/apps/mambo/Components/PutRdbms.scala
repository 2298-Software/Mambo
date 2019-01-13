package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class PutRdbms(spark: SparkSession, config: Config) extends BaseComponent(spark, config) {
  def run(): Boolean = {
    logger.info("executing PutRdbms")
    val jdbcUrl = config.getString("jdbcUrl")
    val saveMode: String = if (config.hasPath("saveMode")) {
      config.getString("saveMode")
    } else {
      "error"
    }

    val df = spark.sql(config.getString("query"))

    df
      .coalesce(config.getInt("parallelConnections"))
      .write
      .format("jdbc")
      .mode(saveMode)
      .option("url", jdbcUrl)
      .option("dbtable", config.getString("outputName"))
      .option("user", config.getString("user"))
      .option("password", config.getString("pass"))
      .option("driver", config.getString("driver"))
      .save()
    true
  }

}
