package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class PutRdbms(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  def run(): Boolean = {
    logger.info("executing PutRdbms")
    val jdbcUrl = config.getString("jdbcUrl")

    spark.sql(config.getString("query")).coalesce(config.getInt("parallelConnections"))
      .write
      .format("jdbc")
      .option("url", jdbcUrl )
      .option("dbtable", config.getString("outputName"))
      .option("user", config.getString("user"))
      .option("password", config.getString("pass"))
      .option("driver", config.getString("driver"))
      .save()
    true
  }

}
