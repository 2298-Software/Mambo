package com.ttne.mambo.processors

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class GetRdbms(spark: SparkSession, config: Config) extends BaseProcessor(spark, config){
  val outputName: String = config.getString("outputName")

  override def run(): Unit = {
    logger.info("executing com.ttne.mambo.processors.GetRdbms")
    val jdbcUrl = config.getString("jdbcUrl")
    val partitionColumn = if(jdbcUrl.toLowerCase.contains("sqlserver")){
      "%s %% %s".format(config.getString("splitColumn"), config.getString("splitCount"))
    } else {
      "mod(%s,%s)".format(config.getString("splitColumn"), config.getString("splitCount"))
    }
    val splitCount: Int = config.getInt("splitCount")

    setDataFrame(spark.read
      .format("jdbc")
      .option("url", jdbcUrl )
      .option("dbtable", "(%s) as t".format(config.getString("query")))
      .option("user", config.getString("user"))
      .option("password", config.getString("pass"))
      .option("driver", config.getString("driver"))
      .option("numPartitions", config.getString("splitCount"))
      .option("partitionColumn", partitionColumn)
      .option("lowerBound", "0")
      .option("upperBound", (splitCount + 1).toString)
      .load()
      , outputName)
  }

}
