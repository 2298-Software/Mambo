package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by joe on 1/1/2019.
  */
class Process(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val sql: String = config.getString("sql")

  def executeSql(): Unit = {
    logger.info("running executeSql")
    setDataFrame(spark.sql(sql))
  }
}
