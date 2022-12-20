package com.ttne.mambo.datasource

import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.{DataSourceWriteOptions, HoodieWriterUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object Hudi {
  val DATASOURCE_NAME = "hudi"

  def getHudiStream(spark: SparkSession, opts: mutable.Map[String, String]): DataFrame = {
    val readStream = spark.readStream
    if (opts.default("inputSmokeTest").equals("true")) {
      readStream
        .format("rate")
        .option("rowsPerSecond", 3)
        .options(opts)
        .load()
    } else {
      readStream
        .format(source = DATASOURCE_NAME)
        .options(opts)
        .load()
    }
  }

  def putHudiStream(df: DataFrame, opts: mutable.Map[String, String]): Unit = {
    //val hudiConfig: HoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(opts)
    val OPERATION = opts.getOrElse("OPERATION", "insert_overwrite_table")
    val PRECOMBINE_FIELD = opts.getOrElse("PRECOMBINE_FIELD", "ts")
    val RECORDKEY_FIELD = opts.getOrElse("RECORDKEY_FIELD", "id")
    val PARTITIONPATH_FIELD = opts.getOrElse("PARTITIONPATH_FIELD", "part")

    val hudiWriter = df.writeStream
    if (opts.default("outputSmokeTest").equals("true")) {
      hudiWriter
        .format("console")
    } else {
      hudiWriter
        .options(getQuickstartWriteConfigs)
        .option(DataSourceWriteOptions.OPERATION.key(), OPERATION)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), PRECOMBINE_FIELD)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), RECORDKEY_FIELD)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), PARTITIONPATH_FIELD)
        .format(DATASOURCE_NAME)
    }
      .start()
      .awaitTermination()

  }
}
