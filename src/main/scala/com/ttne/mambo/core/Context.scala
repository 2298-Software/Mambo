package com.ttne.mambo.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Context {

  def getSpark(conf: SparkConf, opts: mutable.Map[String, String]): SparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .master("local[2]")
      .appName("test")
      .getOrCreate()
  }

  def getLocalSpark(conf: SparkConf, opts: mutable.Map[String, String]): SparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .master("local[2]")
      .appName("local")
      .getOrCreate()
  }

  def getTestSpark(conf: SparkConf, opts: mutable.Map[String, String]): SparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .master("local[2]")
      .appName("test")
      .getOrCreate()
  }
}
