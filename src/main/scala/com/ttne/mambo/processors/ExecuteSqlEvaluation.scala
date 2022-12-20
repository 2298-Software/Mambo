package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ExecuteSqlEvaluation(spark: SparkSession, config: Config) extends BaseProcessor(spark, config){
  val query: String = config.getString("query")
  val failOn: String = config.getString("failOn")

  override def run(): Unit = {
    logger.info("running com.ttne.mambo.processors.ExecuteSqlEvaluation")
    val df = spark.sql(query)
    if(df.count() != 1) {
      throw new Exception("com.ttne.mambo.processors.ExecuteSqlEvaluation expects exactly one result!")
    }

    if(df.first().getString(0).equals(failOn)){
      throw new Exception("com.ttne.mambo.processors.ExecuteSqlEvaluation query (%s) failed the evaluation by returning %s".format(query, failOn))
    }
  }
}