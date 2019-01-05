package main.scala.com.trite.apps.turbine


import java.io.File

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory, ConfigUtil}
import main.scala.com.trite.apps.turbine.Runner.Runner
import org.apache.log4j
import org.apache.spark.sql.SparkSession

/**
  * Created by joe on 1/1/2019.
  */


object TurbineMain {
      def main(args: Array[String]): Unit = {
            val tm = new TurbineMain(args)
            tm.run
      }
}

class TurbineMain(args: Array[String]) {
      def run {
            println("Running Main")
            val logger: Logger = LoggerFactory.getLogger(this.getClass)

            logger.info("Envelope application started ")

            var config: Config = ConfigFactory.parseFile(new File(args(0)))
            if (args.length == 2) {
                 config = config.withFallback(ConfigFactory.parseFile(new File(args(1)))).resolve()
            } else if (args.length > 2) {
                  logger.error("Too many parameters to Turbine application")
            } else {
                  //ConfigUtils.applySubstitutions(config);
            }
            logger.info("Configuration loaded")

            val r = new Runner
            r.run(config)
      }
}
