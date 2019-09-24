package com.twenty298.apps.mambo.Utils

import org.apache.spark.sql.types._

/**
  * Created by joe on 9/20/2019.
  */
object DataFrameUtils {

  def schemaFromString(schemaString: String): StructType ={
    StructType(schemaString.split(",").map(column => StructField(column, inferType(column), true)))
  }

  def inferType(field: String) = field.split(":")(1) match {
    case "Int" => IntegerType
    case "Double" => DoubleType
    case "String" => StringType
    case _ => StringType
  }
}
