package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ExecuteSql(spark: SparkSession, config: Config) extends BaseProcessor(spark, config){
  val query: String = config.getString("query")
  val outputName: String = config.getString("outputName")

  override def run(): Unit = {
    logger.info("running executeSql")
    setDataFrame(spark.sql(query), outputName)
  }
}
