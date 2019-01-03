package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
/**
  * Created by joe on 1/1/2019.
  */
class Process(spark: SparkSession, config: Config){
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val sql = config.getString("sql")
  val outputName = config.getString("outputName")

  def execute(): Unit = {
    println("executing process")
  }

  def executeSql(): Unit = {
    logger.info("executing sql dataset: %s".format(outputName))
    val df: DataFrame = spark.sql(sql)
    setDataFrame(df)
  }

  def setDataFrame(_df: DataFrame): Unit = {
    _df.createOrReplaceTempView(outputName)

    if(config.hasPath("show")){
      _df.show(config.getBoolean("show"))
    }

    if(config.hasPath("repartition")){
      logger.info("repartitioning dataset: %s".format(outputName))
      _df.repartition(config.getInt("repartition"))
    }

    if(config.hasPath("cache")){
      if(config.getBoolean("cache"))
        logger.info("caching dataset: %s".format(outputName))
      _df.cache()
    }
  }
}
