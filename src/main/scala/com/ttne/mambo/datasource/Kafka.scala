package com.ttne.mambo.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object Kafka {
  def getKafkaStream(spark: SparkSession, opts: mutable.Map[String, String]): DataFrame = {
    spark
      .readStream
      .format(source = "kafka")
      .options(opts)
      .load()
  }

  def putKafkaStream(df: DataFrame, opts: mutable.Map[String, String]): Unit = {
    val kafkaWriter = df.writeStream
    if (opts.default("smoketest").equals("true")) {
      kafkaWriter
        .format("console")
    } else {
      kafkaWriter
        .format("hudi")
    }
      .start()
      .awaitTermination()
  }
}
