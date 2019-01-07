package com.trite.apps.turbine


import java.io.File
import java.nio.file.{Files, Path, Paths}

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}
import com.trite.apps.turbine.Runner.Runner

object TurbineMain {
      def main(args: Array[String]): Unit = {
            val tm = new TurbineMain(args)
            tm.run()
      }
}

class TurbineMain(args: Array[String]) {
      def run() {
            val logger: Logger = LoggerFactory.getLogger(this.getClass)

            if (args.length < 1) {
                  throw new RuntimeException("Missing configuration file argument.")
            } else {
                  val p: Path = Paths.get(args(0))
                  if (Files.notExists(p) || Files.isDirectory(p)) {
                        throw new RuntimeException("Can't access pipeline configuration file '" + args(0) + "'.")
                  }
            }

            logger.info("Turbine application started ")

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
