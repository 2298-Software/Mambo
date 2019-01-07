package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenerateDataset(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val outputName: String = config.getString("outputName")

  def run(): Boolean = {
    val numRows = if(config.hasPath("numRows")){
      config.getInt("numRows")
    } else
      20

    logger.info("generating dataset: %s".format(outputName))
    val df: DataFrame = spark.range(0, numRows).toDF()
    setDataFrameWithOutput(df, outputName)
    true
  }
}
