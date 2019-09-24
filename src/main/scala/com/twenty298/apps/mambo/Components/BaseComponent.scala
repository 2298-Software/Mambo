package com.twenty298.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


 class BaseComponent(spark: SparkSession, config: Config) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val name: String = config.getString("enabled")
  val description: String = config.getString("description")
  val enabled: Boolean = config.getBoolean("enabled")
  val typ: String = config.getString("type")

  def setDataFrame(_df: DataFrame, _outputName: String): DataFrame = {
    val df: DataFrame = beforeSet(_df)
    df.createOrReplaceTempView(_outputName)

    logger.info("Spark Database List:")
    spark.catalog.listDatabases().show(false)

    logger.info("Spark Table List:")
    spark.catalog.listTables("default").show(false)

    afterSet(df)
    df
  }

  private def beforeSet(_df: DataFrame) = {
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
      import collection.JavaConversions._
      config.getStringList("dropColumns").toList.foreach({
        e =>
          logger.info("dropping column: %s".format(e))
          df = df.drop(df.col(e))
      })
    }

    df
  }

  private def afterSet(df: DataFrame) = {
    if (config.hasPath("show")) {
      df.show(config.getBoolean("show"))
    }

    val partCount = df.rdd.partitions.length
    logger.info("partition size is: %s".format(partCount))

  }

  def run(): Boolean = {
    true
  }

}
