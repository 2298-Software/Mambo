package com.twenty298.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class ExecuteCdc(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val query1: String = config.getString("query1")
  val query2: String = config.getString("query2")
  val keyColumn: String = config.getString("keyColumn")
  val outputName: String = config.getString("outputName")

  override def run(): Boolean = {
    logger.info("running ExecuteCdc")
    val df = spark.sql(query1).as("t1").join(spark.sql(query2).as("t2"), col("t1.%s".format(keyColumn)) === col("t2.%s".format(keyColumn)), "outer")
    val df_unchanged = df.select("t1.*").where(col("t1.%s".format(keyColumn)).isNotNull and col("t2.%s".format(keyColumn)).isNull)
    val df_updated = df.select("t2.*").where(col("t1.%s".format(keyColumn)).isNotNull and col("t2.%s".format(keyColumn)).isNotNull)
    val df_added = df.select("t2.*").where(col("t1.%s".format(keyColumn)).isNull and col("t2.%s".format(keyColumn)).isNotNull)

    setDataFrame(df, outputName + "_full")
    setDataFrame(df_added, outputName + "_adds")
    setDataFrame(df_updated, outputName + "_updates")
    setDataFrame(df_unchanged, outputName + "_unchanged")
    true
  }
}
