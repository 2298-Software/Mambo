package com.twentytwoninteyeightsoftware.apps.mambo.Components

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._

class GetRedis(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val redisServerDnsAddress: String = config.getString("hostname") //"REDIS_HOSTNAME"
  val redisPortNumber: Int = config.getInt("port") // 6379
  val redisPassword: String = config.getString("password") // "REDIS_PASSWORD"
  val redisPartition: Int = config.getInt("partition") // "3"
  val redisKey: String = config.getString("redisKey") // "key*"
  val redisConfig = new RedisConfig(RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))
  val outputName: String = config.getString("outputName")

  def run(): Boolean = {
    logger.info("executing GetRedis")
    val keysRDD = spark.sparkContext.fromRedisKeyPattern(redisKey, redisPartition)(redisConfig)
    import spark.sqlContext.implicits._
    setDataFrame(keysRDD.getKV.toDF("key", "value"), outputName)
    true
  }
}
