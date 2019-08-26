package com.gilcu2.dataframegenerator

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Random
import org.apache.spark.sql.functions._

object RandomDataFrame {

  def generate(size: Int, keysSize: Int, sizeOtherColumns: Int)(implicit spark: SparkSession):
  (DataFrame, DataFrame) = {
    import spark.implicits._

    val randomUdf = udf((x: Int) => Random.nextInt)
    val totalColumns = keysSize + sizeOtherColumns
    val columnNames = (1 to totalColumns).map("field" + _)

    val initialOneKeyDf = (1 to size)
      .map(i => (i))
      .toDF(columnNames(0))

    var finalRandomValuesOtherColumnsDf = initialOneKeyDf
    (1 to totalColumns - 1).foreach(i => {
      finalRandomValuesOtherColumnsDf = finalRandomValuesOtherColumnsDf.withColumn(columnNames(i), randomUdf($"field1"))
    })

    finalRandomValuesOtherColumnsDf.cache()

    val firstColumnName = columnNames(0)
    val lastColumnName = columnNames(totalColumns - 1)
    val differentDf = finalRandomValuesOtherColumnsDf.withColumn(lastColumnName,
      when(col(firstColumnName) === size, col(lastColumnName) + 1).otherwise(col(lastColumnName))
    )

    (finalRandomValuesOtherColumnsDf, differentDf)
  }

}
