package com.gilcu2.experiments

import com.gilcu2.dataframeequality.{EqualityByExcept, EqualityByHashcode}
import com.gilcu2.dataframegenerator.RandomDataFrame
import com.gilcu2.interfaces.Time._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.Duration

case class PerformanceResult(sizes: Seq[Int], nKeys: Int, nOtherFields: Int,
                             byExceptEqual: Seq[Duration], byExceptDifferent: Seq[Duration],
                             byHashcodeEqual: Seq[Duration], byHashcodeDifferent: Seq[Duration],
                             byDirectComparisonEqual: Seq[Duration], byDirectComparisonDifferent: Seq[Duration]
                            ) {
  override def toString: String = {

    def makeString[T](label: String, values: Seq[T]): String = s"$label\t\t\t${values.mkString("\t")}\n"

    def makeStringDuration(label: String, values: Seq[Duration]): String =
      makeString(label, values.map(_.getMillis))

    makeString("Size", sizes) + "\n" +
      makeStringDuration("ExceptEqual", byExceptEqual) +
      makeStringDuration("HashcodeEqual", byHashcodeEqual) +
      makeStringDuration("ComparisonEqual", byDirectComparisonEqual) + "\n" +
      makeStringDuration("ExceptDifferent", byExceptDifferent) +
      makeStringDuration("HashcodeDifferent", byHashcodeDifferent) +
      makeStringDuration("ComparisonDifferent", byDirectComparisonDifferent)

  }
}

object Performance {


  def measureTimesPerAlgorithm(sizes: Seq[Int], nKeys: Int, nOtherFields: Int)(implicit spark: SparkSession): PerformanceResult = {

    val sizes1 = Seq(sizes.head) ++ sizes

    val times = sizes1.map(size => {
      val (df1, df2) = RandomDataFrame.generate(size, nKeys, nOtherFields)
      df1.cache()
      df2.cache()
      df1.count()
      df2.count()

      val keyFieldNames = df1.columns.slice(0, nKeys - 1)

      measureTimesPerData(df1, df2, keyFieldNames)

    })

    getResults(sizes, nKeys, nOtherFields, times)
  }

  private def getResults(sizes: Seq[Int], nKeys: Int, nOtherFields: Int, times: Seq[TimeResultsPerData]) = {

    val timesByExceptEqual = times.map(_.timeByExceptEqual)
    val timesByExceptDifferent = times.map(_.timeByExceptDifferent)

    val timesByHashcodeEqual = times.map(_.timeByHashCodeEqual)
    val timesByHashcodeDifferent = times.map(_.timeByHashcodeDifferent)

    val timesDirectComparisonEqual = times.map(_.timeByDirectComparisonEqual)
    val timesByDirectComparisonDifferent = times.map(_.timeByDirectComparisonDifferent)

    PerformanceResult(sizes, nKeys, nOtherFields,
      timesByExceptEqual, timesByExceptDifferent,
      timesByHashcodeEqual, timesByHashcodeDifferent,
      timesDirectComparisonEqual, timesByDirectComparisonDifferent)
  }

  private def measureTimesPerData(df1: DataFrame, df2: DataFrame, keyFieldNames: Array[String])(
    implicit spark: SparkSession): TimeResultsPerData = {

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

    TimeResultsPerData(timeByExceptEqual, timeByExceptDifferent, timeByHashCodeEqual, timeByHashcodeDifferent, timeByDirectComparisonEqual, timeByDirectComparisonDifferent)
  }

  private def measureTimeAndValidate(f: (DataFrame, DataFrame, Array[String]) => Boolean,
                                     df1: DataFrame, df2: DataFrame, keyFieldNames: Array[String],
                                     expected: Boolean)(implicit spark: SparkSession): Duration = {
    val beginTime = getCurrentTime
    val result = f(df1, df2, keyFieldNames)
    val endTime = getCurrentTime

    assert(result == expected)

    getDuration(beginTime, endTime)
  }

  private case class TimeResultsPerData(timeByExceptEqual: Duration, timeByExceptDifferent: Duration,
                                        timeByHashCodeEqual: Duration, timeByHashcodeDifferent: Duration,
                                        timeByDirectComparisonEqual: Duration, timeByDirectComparisonDifferent: Duration)

}
