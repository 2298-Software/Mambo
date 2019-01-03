package main.scala.com.trite.apps.turbine.Runner

import com.typesafe.config.Config
import main.scala.com.trite.apps.turbine.Components._
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by joe on 1/1/2019.
  */
class Runner {
  def run(config: Config) {

    import org.apache.spark.SparkConf
    val sparkConf = new SparkConf
    sparkConf.setAppName(config.getString("application.name"))
    sparkConf.setMaster(config.getString("application.master"))

    if (config.hasPath("application.executors")) {
      sparkConf.set("spark.executor.instances", config.getString("application.executors"))
    }
    if (config.hasPath("application.executor.cores")) {
      sparkConf.set("spark.executor.cores", config.getString("application.executor.cores"))
    }
    if (config.hasPath("application.executor.memory")) {
      sparkConf.set("spark.executor.memory", config.getString("application.executor.memory"))
    }

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()


    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    logger.debug("Starting getting steps")

    import collection.JavaConversions._

    val stepNames = config.getObject("steps").keySet().toSeq.sorted

    for (stepName: String <- stepNames) {
      val stepConfig: Config = config.getConfig("steps").getConfig(stepName);
      val typ = stepConfig.getValue("type").unwrapped()
      val subType = stepConfig.getValue("subType").unwrapped()

      typ
      match {
        case "generate" => {
          val c = new Generate(spark, stepConfig)
          subType
          match {
            case "dataset" => {
              c.dataset()
            }
          }

        }
        case "ingest" => {
          val c = new Ingest(spark, stepConfig)
          c.execute
        }
        case "process" => {
          val c = new Process(spark, stepConfig)
          c.execute()
        }
        case "distribute" => {
          val c = new Distribute(spark,   stepConfig)
          subType
          match {
            case "saveFile" => {
              c.saveFile()
            }
          }
        }
      }

    }
  }
}
