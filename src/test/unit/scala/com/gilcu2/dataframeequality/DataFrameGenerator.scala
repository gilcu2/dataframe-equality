package com.gilcu2.dataframeequality

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class DataFrameGeneratorResult(df1: DataFrame, df2: DataFrame, keyFields: Array[String])

object DataFrameGenerator {

  def generateDataFrames(equal: Boolean)(implicit spark: SparkSession): DataFrameGeneratorResult = {

    val keyField1 = "key1"
    val keyField2 = "key2"
    val valueField1 = "value1"
    val valueField2 = "value2"

    val schema = List(
      StructField(keyField1, IntegerType, true),
      StructField(keyField2, StringType, true),
      StructField(valueField1, StringType, true),
      StructField(valueField2, IntegerType, true)
    )

    Seq(
      Row(8, "9", "bat", 3),
      Row(64, "23", "mouse", 4),
      Row(-27, "41", "horse", 7)
    )

    val data1 = Seq(
      Row(8, "9", "bat", 3),
      Row(64, "23", "mouse", 4),
      Row(-27, "41", "horse", 7)
    )

    val data2 = if (equal) data1 else
      Seq(
        Row(8, "9", "bat", 3),
        Row(64, "23", "mouse", 4),
        Row(-27, "41", "horse", 8)
      )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      StructType(schema)
    )
    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(data2),
      StructType(schema)
    )

    DataFrameGeneratorResult(df1, df2, Array(keyField1, keyField2))
  }

}
