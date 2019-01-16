package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.redislabs.provider.redis._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class PutRedis(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val redisServerDnsAddress: String = config.getString("hostname") //"REDIS_HOSTNAME"
  val redisPortNumber: Int = config.getInt("port") // 6379
  val redisPassword: String = config.getString("password") // "REDIS_PASSWORD"
  val redisPartition: Int = config.getInt("partition") // "3"
  val redisKey: String = config.getString("redisKey") // "key*"
  val redisConfig = new RedisConfig(RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))
  val query: String = config.getString("query")

  def run(): Boolean = {
    logger.info("executing PutRedis")
    import spark.sqlContext.implicits._

    val df = spark.sql(query)

    if(df.columns.length != 2){
      throw new Exception("PutRedis is expect two columns (key and value).")
    }

    spark.sparkContext.toRedisKV(df.map(x => (x.getString(0), x.getString(1))).rdd)(redisConfig)
    true
  }
}
