package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class BaseComponent(spark: SparkSession, config: Config){
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def setDataFrame(_df: DataFrame): DataFrame = {
    val df = if(config.hasPath("repartition")){
      logger.info("repartitioning dataset")
      _df.repartition(config.getInt("repartition"))
    } else _df

    if(config.hasPath("cache")){
      if(config.getBoolean("cache"))
        logger.info("caching dataset")
      df.cache()
    }

    if(config.hasPath("show")){
      df.show(config.getBoolean("show"))
    }
    val partCount = df.rdd.partitions.length
    logger.info("partition size is: %s".format(partCount))

    df
  }

  def setDataFrameWithOutput(_df: DataFrame, _outputName: String): DataFrame = {
    val df = if(config.hasPath("repartition")){
      logger.info("repartitioning dataset")
      _df.repartition(config.getInt("repartition"))
    } else _df

    if(config.hasPath("cache")){
      if(config.getBoolean("cache"))
        logger.info("caching dataset")
      df.cache()
    }

    df.createOrReplaceTempView(_outputName)

    if(config.hasPath("show")){
      df.show(config.getBoolean("show"))
    }
    val partCount = df.rdd.partitions.length
    logger.info("partition size is: %s".format(partCount))

    df
  }
}
