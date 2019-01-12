package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class PutRdbms(spark: SparkSession, config: Config) extends BaseComponent(spark, config){

  override def run(): Boolean = {
    logger.info("executing PutRdbms")
    val jdbcUrl = config.getString("jdbcUrl")
    val partitionColumn = if(jdbcUrl.toLowerCase.contains("sqlserver")){
      "%s %% %s".format(config.getString("splitColumn"), config.getString("splitCount"))
    } else {
      "mod(%s,%s)".format(config.getString("splitColumn"), config.getString("splitCount"))
    }
    val splitCount: Int = config.getInt("splitCount")

    spark.sql(config.getString("query"))
        .write
      .format("jdbc")
      .option("url", jdbcUrl )
      .option("dbtable", config.getString("outputName"))
      .option("user", config.getString("user"))
      .option("password", config.getString("pass"))
      .option("driver", config.getString("driver"))
      .option("numPartitions", config.getString("splitCount"))
      .option("partitionColumn", partitionColumn)
      .option("lowerBound", "0")
      .option("upperBound", (splitCount + 1).toString)
      .save()

    true
  }

}
