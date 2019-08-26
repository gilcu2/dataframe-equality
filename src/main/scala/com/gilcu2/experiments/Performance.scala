package com.gilcu2.experiments

import com.gilcu2.dataframeequality.{EqualityByExcept, EqualityByHashcode}
import com.gilcu2.dataframegenerator.RandomDataFrame
import com.gilcu2.interfaces.Time._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.Duration

case class PerformanceResult(nRows: Seq[Int], nKeys: Int, nOtherFields: Int,
                             byExceptEqual: Seq[Duration], byExcexptDifferent: Seq[Duration],
                             byHashcodeEqual: Seq[Duration], byHashcodeDifferent: Seq[Duration],
                             byDirectComparizonEqual: Seq[Duration], DirectComparizon: Seq[Duration]
                            )

object Performance {


  def measureTimes(sizes: Seq[Int], nKeys: Int, nOtherFields: Int)(implicit spark: SparkSession): PerformanceResult = {

    val times = sizes.map(size => {
      val (df1, df2) = RandomDataFrame.generate(size, nKeys, nOtherFields)
      val keyFieldNames = df1.columns.slice(0, nKeys - 1)

      measureTimesEqualAndDifferent(df1, df2, keyFieldNames)

    })

    val timesByExceptEqual

    PerformanceResult(Seq(1), 1, 1,
      Seq.empty[Duration], Seq.empty[Duration], Seq.empty[Duration],
      Seq.empty[Duration], Seq.empty[Duration], Seq.empty[Duration])
  }

  private def measureTimesEqualAndDifferent(df1: DataFrame, df2: DataFrame, keyFieldNames: Array[String])(
    implicit spark: SparkSession): Array[Duration] = {

    val timeByExceptEqual = measureTimeAndValidate(EqualityByExcept.areEqual,
      df1, df1, keyFieldNames, expected = true)
    val timeByExceptDifferent = measureTimeAndValidate(EqualityByExcept.areEqual,
      df1, df2, keyFieldNames, expected = false)

    val timeByHashCodeEqual = measureTimeAndValidate(EqualityByHashcode.areEqual,
      df1, df1, keyFieldNames, expected = true)
    val timeByHashcodeDifferent = measureTimeAndValidate(EqualityByHashcode.areEqual,
      df1, df2, keyFieldNames, expected = false)

    val timeByDirectComparisonEqual = measureTimeAndValidate(EqualityByHashcode.areEqual,
      df1, df1, keyFieldNames, expected = true)
    val timeByDirectComparisonDifferent = measureTimeAndValidate(EqualityByHashcode.areEqual,
      df1, df2, keyFieldNames, expected = false)

    Array(timeByExceptEqual, timeByExceptDifferent, timeByHashCodeEqual, timeByHashcodeDifferent, timeByDirectComparisonEqual, timeByDirectComparisonDifferent)
  }

  private def measureTimeAndValidate(f: (DataFrame, DataFrame, Array[String]) => Boolean,
                                     df1: DataFrame, df2: DataFrame, keyFieldNames: Array[String],
                                     expected: Boolean)(implicit spark: SparkSession): Duration = {
    val beginTime = getCurrentTime
    val result = f(df1, df1, keyFieldNames)
    val endTime = getCurrentTime

    assert(result == expected)

    getDuration(beginTime, endTime)
  }
}
