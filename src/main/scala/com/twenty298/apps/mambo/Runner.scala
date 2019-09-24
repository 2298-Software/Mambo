package com.twenty298.apps.mambo

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.SparkSession
import com.twenty298.apps.mambo.Components.BaseComponent
import scala.util.{Failure, Success, Try}

class Runner {
  def run(config: Config) {

    import org.apache.spark.SparkConf
    val sparkConf = new SparkConf
    sparkConf.setAppName(config.getString("application.name"))
    sparkConf.setMaster(config.getString("application.master"))

    //set configs from the "spark" section of the config
    import scala.collection.JavaConversions._
    config.getConfig("spark").entrySet().map(e => {
      val key = e.getKey.replace("_",".")
      val value = e.getValue.unwrapped().toString
      println("setting spark configuration value for %s to %s".format(key, value))
      sparkConf.set(key, value )
    })

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
      logger.info("Found step: {}", stepName)
      val stepConfig: Config = config.getConfig("steps").getConfig(stepName)
      val typ = stepConfig.getString("type")
      val enabled = stepConfig.getBoolean("enabled")

      if (enabled) {
        logger.info("Step: {} enabled, executing..", stepName)
        val processor = Try(Class.forName(s"com.twenty298.apps.mambo.Components.$typ"))
        match {
          case Success(value) => value.getConstructor(classOf[SparkSession], classOf[Config]).newInstance(spark, stepConfig).asInstanceOf[BaseComponent]
          case Failure(e) => throw new Exception("step type com.twenty298.apps.mambo.Components.%s not implemented!".format(stepConfig.getString("type")))
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
