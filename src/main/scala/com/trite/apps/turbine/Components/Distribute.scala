package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
/**
  * Created by joe on 1/1/2019.
  */
class Distribute(spark: SparkSession, config: Config){
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val path = config.getString("path")
  val format = config.getString("format")
  val sql = config.getString("sql")

  val saveMode = if(config.hasPath("saveMode")) {
    config.getString("saveMode")
  } else {
    "error"
  }


  def execute(): Unit = {
    println("executing distribute")
  }

  def saveFile(): Unit = {
    logger.info("distribute dataset")
    val df: DataFrame = spark.sql(sql)
    setDataFrame(df).write.mode(saveMode).format(format).save(path)
  }

  def setDataFrame(_df: DataFrame): DataFrame = {
    if(config.hasPath("show")){
      _df.show(config.getBoolean("show"))
    }

    if(config.hasPath("repartition")){
      logger.info("repartitioning dataset")
      _df.repartition(config.getInt("repartition"))
    }

    if(config.hasPath("cache")){
      if(config.getBoolean("cache"))
        logger.info("caching dataset")
      _df.cache()
    }

    _df
  }
}
