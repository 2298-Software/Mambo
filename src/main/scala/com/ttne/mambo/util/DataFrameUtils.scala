package com.ttne.mambo.util

import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType}

object DataFrameUtils {

  def schemaFromString(schemaString: String): StructType = {
    StructType(schemaString.split(",").map(column => StructField(column, inferType(column), nullable = true)))
  }

  def inferType(field: String): DataType = {
    field.split(":")(1) match {
      case "Int" => IntegerType
      case "Double" => DoubleType
      case "String" => StringType
      case _ => StringType
    }
  }
}
