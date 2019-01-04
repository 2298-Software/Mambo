package main.scala.com.trite.apps.turbine.Components

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by joe on 1/1/2019.
  */
class Generate(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  def dataset(): Unit = {
    val numRows = if(config.hasPath("numRows")){
      config.getInt("numRows")
    } else
      20

    logger.info("generating dataset: %s".format(outputName))
    val df: DataFrame = spark.range(0, numRows).toDF()
    setDataFrame(df)
  }
}
