package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenerateDataset(spark: SparkSession, config: Config) extends BaseProcessor(spark, config){
  val outputName: String = config.getString("outputName")

  override def run(): Unit = {
    val numRows = if(config.hasPath("numRows")){
      config.getInt("numRows")
    } else
      20

    logger.info("generating dataset: %s".format(outputName))
    val df: DataFrame = spark.range(0, numRows).toDF()
    setDataFrame(df, outputName)
  }
}
