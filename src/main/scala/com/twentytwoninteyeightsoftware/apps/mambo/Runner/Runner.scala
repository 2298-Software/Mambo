package com.twentytwoninteyeightsoftware.apps.mambo.Runner

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.twentytwoninteyeightsoftware.apps.mambo.Components._
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
    logger.info("\n          _____                    _____                    _____                    _____                   _______         \n         /\\    \\                  /\\    \\                  /\\    \\                  /\\    \\                 /::\\    \\        \n        /::\\____\\                /::\\    \\                /::\\____\\                /::\\    \\               /::::\\    \\       \n       /::::|   |               /::::\\    \\              /::::|   |               /::::\\    \\             /::::::\\    \\      \n      /:::::|   |              /::::::\\    \\            /:::::|   |              /::::::\\    \\           /::::::::\\    \\     \n     /::::::|   |             /:::/\\:::\\    \\          /::::::|   |             /:::/\\:::\\    \\         /:::/~~\\:::\\    \\    \n    /:::/|::|   |            /:::/__\\:::\\    \\        /:::/|::|   |            /:::/__\\:::\\    \\       /:::/    \\:::\\    \\   \n   /:::/ |::|   |           /::::\\   \\:::\\    \\      /:::/ |::|   |           /::::\\   \\:::\\    \\     /:::/    / \\:::\\    \\  \n  /:::/  |::|___|______    /::::::\\   \\:::\\    \\    /:::/  |::|___|______    /::::::\\   \\:::\\    \\   /:::/____/   \\:::\\____\\ \n /:::/   |::::::::\\    \\  /:::/\\:::\\   \\:::\\    \\  /:::/   |::::::::\\    \\  /:::/\\:::\\   \\:::\\ ___\\ |:::|    |     |:::|    |\n/:::/    |:::::::::\\____\\/:::/  \\:::\\   \\:::\\____\\/:::/    |:::::::::\\____\\/:::/__\\:::\\   \\:::|    ||:::|____|     |:::|    |\n\\::/    / ~~~~~/:::/    /\\::/    \\:::\\  /:::/    /\\::/    / ~~~~~/:::/    /\\:::\\   \\:::\\  /:::|____| \\:::\\    \\   /:::/    / \n \\/____/      /:::/    /  \\/____/ \\:::\\/:::/    /  \\/____/      /:::/    /  \\:::\\   \\:::\\/:::/    /   \\:::\\    \\ /:::/    /  \n             /:::/    /            \\::::::/    /               /:::/    /    \\:::\\   \\::::::/    /     \\:::\\    /:::/    /   \n            /:::/    /              \\::::/    /               /:::/    /      \\:::\\   \\::::/    /       \\:::\\__/:::/    /    \n           /:::/    /               /:::/    /               /:::/    /        \\:::\\  /:::/    /         \\::::::::/    /     \n          /:::/    /               /:::/    /               /:::/    /          \\:::\\/:::/    /           \\::::::/    /      \n         /:::/    /               /:::/    /               /:::/    /            \\::::::/    /             \\::::/    /       \n        /:::/    /               /:::/    /               /:::/    /              \\::::/    /               \\::/____/        \n        \\::/    /                \\::/    /                \\::/    /                \\::/____/                 ~~              \n         \\/____/                  \\/____/                  \\/____/                  ~~                                       \n                                                                                                                             ")

    val renderOpts = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)
    logger.info("\nYour config is:\n" + config.root().render(renderOpts))

    logger.info("Getting steps")

    import collection.JavaConversions._

    val stepNames = config.getObject("steps").keySet().toSeq.sorted

    for (stepName: String <- stepNames) {
      val stepConfig: Config = config.getConfig("steps").getConfig(stepName)
      val typ = stepConfig.getValue("type").unwrapped()

      typ
      match {
        case "GenerateDataset" =>
          new GenerateDataset(spark, stepConfig).run()
        case "GetFile" =>
          new GetFile(spark, stepConfig).run()
        case "ExecuteSql" =>
          new ExecuteSql(spark, stepConfig).run()
        case "GetRdbms" =>
          new GetRdbms(spark, stepConfig).run()
        case "PutFile" =>
          new PutFile(spark, stepConfig).run()
        case "PutRdbms" =>
          new PutRdbms(spark, stepConfig).run()
        case _ =>
          throw new Exception("step type %s not implemented!".format(typ))
      }
    }

    spark.stop()
  }
}
