package com.ttne.mambo.core

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File

object MamboMain {
      def main(args: Array[String]): Unit = {
            val tm = new MamboMain(args)
            tm.run()
      }
}

class MamboMain(args: Array[String]) {
      def run(): Unit = {
            val logger: Logger = LoggerFactory.getLogger(this.getClass)

            if (args.length != 1) {
                  throw new RuntimeException("Missing configuration file argument.")
            }

            logger.info("Mambo application started")

            val filePath = args(0)
            val config: Config = ConfigFactory.parseFile(new File(filePath)).resolve()

            logger.info("Configuration loaded")

            val r = new Runner
            r.run(config)
      }
}
