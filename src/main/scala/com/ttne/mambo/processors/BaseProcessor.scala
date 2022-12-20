package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

 class BaseProcessor(spark: SparkSession, config: Config) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val name: String = config.getString("name")
  val description: String = config.getString("description")
  val enabled: Boolean = config.getBoolean("enabled")
  val typ: String = config.getString("type")

  def setDataFrame(_df: DataFrame, _outputName: String): DataFrame = {
    val df: DataFrame = beforeSet(_df)
    df.createOrReplaceTempView(_outputName)
    afterSet(df)
    df
  }

  private def beforeSet(_df: DataFrame): DataFrame = {
    var df = if (config.hasPath("repartition")) {
      logger.info("repartitioning dataset")
      _df.repartition(config.getInt("repartition"))
    } else _df

    if (config.hasPath("cache")) {
      if (config.getBoolean("cache"))
        logger.info("caching dataset")
      df.cache()
    }

    if (config.hasPath("dropColumns")) {
      val cols =  config.getStringList("dropColumns")
      logger.info("dropping column: %s".format(cols))
      df = df.drop(cols.toString)
    }

    df
  }

  private def afterSet(df: DataFrame): Unit = {
    if (config.hasPath("show")) {
      df.show(config.getBoolean("show"))
    }

    logger.info("Spark Database List:")
    spark.catalog.listDatabases().show(truncate=false)

    logger.info("Spark Table List:")
    spark.catalog.listTables("default").show(truncate=false)

    val partCount = df.rdd.partitions.length
    logger.info("partition size is: %s".format(partCount))

  }

  def run(): Unit = {
  }

}
