package com.ttne.mambo.core

import com.ttne.mambo.processors.BaseProcessor
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class Runner {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("\n          _____                    _____                    _____                    _____                   _______         \n         /\\    \\                  /\\    \\                  /\\    \\                  /\\    \\                 /::\\    \\        \n        /::\\____\\                /::\\    \\                /::\\____\\                /::\\    \\               /::::\\    \\       \n       /::::|   |               /::::\\    \\              /::::|   |               /::::\\    \\             /::::::\\    \\      \n      /:::::|   |              /::::::\\    \\            /:::::|   |              /::::::\\    \\           /::::::::\\    \\     \n     /::::::|   |             /:::/\\:::\\    \\          /::::::|   |             /:::/\\:::\\    \\         /:::/~~\\:::\\    \\    \n    /:::/|::|   |            /:::/__\\:::\\    \\        /:::/|::|   |            /:::/__\\:::\\    \\       /:::/    \\:::\\    \\   \n   /:::/ |::|   |           /::::\\   \\:::\\    \\      /:::/ |::|   |           /::::\\   \\:::\\    \\     /:::/    / \\:::\\    \\  \n  /:::/  |::|___|______    /::::::\\   \\:::\\    \\    /:::/  |::|___|______    /::::::\\   \\:::\\    \\   /:::/____/   \\:::\\____\\ \n /:::/   |::::::::\\    \\  /:::/\\:::\\   \\:::\\    \\  /:::/   |::::::::\\    \\  /:::/\\:::\\   \\:::\\ ___\\ |:::|    |     |:::|    |\n/:::/    |:::::::::\\____\\/:::/  \\:::\\   \\:::\\____\\/:::/    |:::::::::\\____\\/:::/__\\:::\\   \\:::|    ||:::|____|     |:::|    |\n\\::/    / ~~~~~/:::/    /\\::/    \\:::\\  /:::/    /\\::/    / ~~~~~/:::/    /\\:::\\   \\:::\\  /:::|____| \\:::\\    \\   /:::/    / \n \\/____/      /:::/    /  \\/____/ \\:::\\/:::/    /  \\/____/      /:::/    /  \\:::\\   \\:::\\/:::/    /   \\:::\\    \\ /:::/    /  \n             /:::/    /            \\::::::/    /               /:::/    /    \\:::\\   \\::::::/    /     \\:::\\    /:::/    /   \n            /:::/    /              \\::::/    /               /:::/    /      \\:::\\   \\::::/    /       \\:::\\__/:::/    /    \n           /:::/    /               /:::/    /               /:::/    /        \\:::\\  /:::/    /         \\::::::::/    /     \n          /:::/    /               /:::/    /               /:::/    /          \\:::\\/:::/    /           \\::::::/    /      \n         /:::/    /               /:::/    /               /:::/    /            \\::::::/    /             \\::::/    /       \n        /:::/    /               /:::/    /               /:::/    /              \\::::/    /               \\::/____/        \n        \\::/    /                \\::/    /                \\::/    /                \\::/____/                 ~~              \n         \\/____/                  \\/____/                  \\/____/                  ~~                                       \n                                                                                                                             ")

  def run(config: Config): Unit = {

    import org.apache.spark.SparkConf
    val sparkConf = new SparkConf
    sparkConf.setAppName(config.getString("spark.name"))
    sparkConf.setMaster(config.getString("spark.master"))

    val withHive = config.getBoolean("spark.hive")

    logger.info("set configs from the 'spark' section of the config")
    import collection.JavaConverters._
    config.getConfig("spark").entrySet().asScala.map(e => {
      val key = "spark." + e.getKey
      val value = e.getValue.unwrapped().toString
      logger.info("setting spark configuration value for %s to %s".format(key, value))
      sparkConf.set(key, value )
    })

    val builder: SparkSession.Builder = SparkSession.builder()
    val builderConfig = builder.config(sparkConf)

    if (withHive) {
      logger.info("enabling hive support")
      builderConfig.enableHiveSupport()
    }

    val spark = builderConfig.getOrCreate()

    val renderOpts = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)
    logger.info("\nYour config is:\n" + config.root().render(renderOpts))

    logger.info("Getting steps")
    val stepNames = config.getObject("steps").keySet().asScala.toSeq.sorted

    for (stepName: String <- stepNames) {
      logger.info("Found step: {}", stepName)
      val stepConfig: Config = config.getConfig("steps").getConfig(stepName)
      val typ = stepConfig.getString("type")
      val enabled = stepConfig.getBoolean("enabled")
      val clazz = s"com.twenty298.apps.mambo.Processors.$typ"

      if (enabled) {
        logger.info("Step: {} enabled, executing..", stepName)
        val processor = Try(Class.forName(clazz))
        match {
          case Success(value) => value.getConstructor(classOf[SparkSession], classOf[Config]).newInstance(spark, stepConfig).asInstanceOf[BaseProcessor]
          case Failure(e) => throw new Exception("step type %s not implemented!".format(clazz))
        }
        processor.run()

      logger.info("Step: {} is complete.", stepName)
    }

      else {
        logger.info("skipping disabled step: {}", stepConfig.getString("name"))
      }
    }
    spark.stop()
  }
}
