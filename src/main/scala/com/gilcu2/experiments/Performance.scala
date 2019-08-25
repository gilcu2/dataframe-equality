package com.gilcu2.experiments

import com.gilcu2.dataframeequality.EqualityByExcept
import com.gilcu2.dataframegenerator.RandomDataFrame
import com.gilcu2.interfaces.Time._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.Duration

case class PerformanceResult(nRows: Seq[Int], nKeys: Int, nOtherFields: Int,
                             byExceptEqual: Seq[Duration], byExcexptDifferent: Seq[Duration],
                             byHashcodeEqual: Seq[Duration], byHashcodeDifferent: Seq[Duration],
                             byDirectComparizonEqual: Seq[Duration], DirectComparizon: Seq[Duration]
                            )

class Performance {


  def measureTimes(sizes: Seq[Int], nKeys: Int, nOtherFields: Int)(implicit spark: SparkSession): PerformanceResult = {


    val times = sizes.map(size => {
      val (df1, df2) = RandomDataFrame.generate(size, nKeys, nOtherFields)
      val keyFieldNames = df1.columns.slice(0, nKeys - 1)

      val byExceptEqual = measureTime(df1, df1, keyFieldNames, expected = true)
      val byExceptDifferent = measureTime(df1, df1, keyFieldNames, expected = true)

    })
    PerformanceResult()
  }

  private def measureTime(df1: DataFrame, df2: DataFrame, keyFieldNames: Array[String], expected: Boolean): Duration = {
    val beginTime = getCurrentTime
    val result = EqualityByExcept.areEqual(df1, df1, keyFieldNames)
    val endTime = getCurrentTime
    getDuration(beginTime, endTime)
  }
}
