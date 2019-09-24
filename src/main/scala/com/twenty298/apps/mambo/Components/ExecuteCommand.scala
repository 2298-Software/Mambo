package com.twenty298.apps.mambo.Components

import com.twenty298.apps.mambo.Utils.DataFrameUtils
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ExecuteCommand(spark: SparkSession, config: Config) extends BaseComponent(spark, config){
  val command: String = config.getString("command")

  override def run(): Boolean = {
    logger.info("Running ExecuteCommand")
    import sys.process._
    if(config.hasPath("outputName")){
      val schemaString: String = config.getString("schema")
      val outputName: String = config.getString("outputName")
      val commandOut: String = command.!!

      val struct: StructType = DataFrameUtils.schemaFromString(schemaString)
      val cmdRdd: RDD[String] = spark.sparkContext.parallelize(commandOut.split(System.lineSeparator()))
      val cmdRowRdd: RDD[Row] = cmdRdd.map(e => e.split(" ")).map(r => Row(r : _*))
      val cmdDF: DataFrame = spark.sqlContext.createDataFrame(cmdRowRdd, struct)

      setDataFrame(cmdDF, outputName)
    } else {
      val exitCode: Int = command.!
      if(exitCode != 0){
        throw new Exception("non-zero exit code %s returned by ExecuteCommand while executing %s".format(exitCode, command))
      } else {
        logger.info("exit code is: %s".format(exitCode))
      }
    }
    true
  }
}
