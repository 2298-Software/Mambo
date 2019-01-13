package com.twentytwoninteyeightsoftware.apps.mambo.Utils

import org.apache.calcite.schema.Schema
import org.apache.spark.sql.types._


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
