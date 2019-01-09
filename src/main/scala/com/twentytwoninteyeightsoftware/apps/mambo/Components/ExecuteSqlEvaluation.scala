package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ExecuteSqlEvaluation(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val query: String = config.getString("query")
  val failOn: String = config.getString("failOn")

  def run(): Unit = {
    logger.info("running ExecuteSqlEvaluation")
    val df = spark.sql(query)
    if(df.count() != 1) {
      throw new Exception("ExecuteSqlEvaluation expects exactly one result!")
    }

    if(df.first().getString(0).equals(failOn)){
      throw new Exception("ExecuteSqlEvaluation query (%s) failed the evaluation by returning %s".format(query, failOn))
    }
  }
}