package com.gilcu2.dataframegenerator

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Random
import org.apache.spark.sql.functions._

object RandomDataFrame {

  def generate(size: Int, keysSize: Int, sizeOtherColumns: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val randomUdf = udf((x: Int) => Random.nextInt)
    val totalColumns = keysSize + sizeOtherColumns
    val columnNames = (1 to totalColumns).map("field" + _)
    val iniDf = (1 to size)
      .map(i => (i))
      .toDF(columnNames(0))
    var df = iniDf
    (1 to totalColumns - 1).foreach(i => {
      df = df.withColumn(columnNames(i), randomUdf($"field1"))
    })
    df
  }

}
