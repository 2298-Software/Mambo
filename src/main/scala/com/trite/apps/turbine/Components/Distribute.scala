package com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Distribute(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val path: String = config.getString("path")
  val format: String = config.getString("format")
  val sql: String = config.getString("sql")

  val saveMode: String = if(config.hasPath("saveMode")) {
    config.getString("saveMode")
  } else {
    "error"
  }

  def saveFile(): Unit = {
    logger.info("executing saveFile")
    val df: DataFrame = spark.sql(sql)
    setDataFrame(df).write.mode(saveMode).format(format).save(path)
  }
}
