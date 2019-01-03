package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
/**
  * Created by joe on 1/1/2019.
  */
class Ingest(spark: SparkSession, config: Config){
  def execute(): Unit = {
    println("executing ingest for: %s".format(config.getValue("path").unwrapped()))
  }
}
