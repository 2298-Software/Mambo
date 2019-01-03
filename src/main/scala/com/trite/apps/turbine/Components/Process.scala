package main.scala.com.trite.apps.turbine.Components
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
/**
  * Created by joe on 1/1/2019.
  */
class Process(spark: SparkSession, config: Config){
  def execute(): Unit = {
    println("executing process")
  }
}
