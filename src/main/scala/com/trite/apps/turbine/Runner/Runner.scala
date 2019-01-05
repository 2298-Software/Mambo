package com.trite.apps.turbine.Runner

import com.typesafe.config.Config
import com.trite.apps.turbine.Components._
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.SparkSession

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
      val stepConfig: Config = config.getConfig("steps").getConfig(stepName)
      val typ = stepConfig.getValue("type").unwrapped()
      val subType = stepConfig.getValue("subType").unwrapped()

      typ
      match {
        case "generate" =>
          val gen = new Generate(spark, stepConfig)
          subType
          match {
            case "dataset" =>
              gen.dataset()
          }
        case "ingest" =>
          val ing = new Ingest(spark, stepConfig)
          subType
          match {
            case "importFile" =>
              ing.importFile()
            case "importCsvFile" =>
              ing.importCsvFile()
            case "importXlsFile" =>
              ing.importXlsFile()
          }
        case "process" =>
          val pro = new Process(spark, stepConfig)
          subType
          match {
            case "executeSql" =>
              pro.executeSql()
          }
        case "distribute" =>
          val dist = new Distribute(spark, stepConfig)
          subType
          match {
            case "saveFile" =>
              dist.saveFile()
          }
      }
    }
  }
}
