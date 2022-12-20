package com.ttne.mambo.processors

import com.ttne.mambo.datasource.Kafka
import com.ttne.mambo.datasource.{Hudi, Kafka}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object KakfaToHudi {

  def execute(spark: SparkSession, opts: mutable.Map[String, String]): Unit = {
    val df = Kafka.getKafkaStream(spark, opts)

    val transformedDf = transform(df)
    Hudi.putHudiStream(transformedDf, opts)
  }

  def transform(df: DataFrame): DataFrame = {
    df.withColumn("new_data", lit("test"))
  }

}
