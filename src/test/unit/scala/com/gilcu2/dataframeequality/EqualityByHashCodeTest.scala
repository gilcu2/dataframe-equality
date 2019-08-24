package com.gilcu2.dataframeequality

import com.gilcu2.dataframeequality.DataFrameGenerator.generateDataFrames
import com.gilcu2.dataframeequality.EqualityByHashcode._
import com.gilcu2.testUtil.SparkSessionTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class EqualityByHashCodeTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "EqualityByHashCode"

  it should "return false when the dataframes have different rows" in {

    Given("two dataframes with the same scheme and different rows")

    val DataFrameGeneratorResult(df1, df2, keyFields) = generateDataFrames(equal = false)

    When("the dataframes are compared")
    val result = areEqual(df1, df2, keyFields)

    Then("the reult must be false")
    result shouldBe false
  }

  it should "return true when the dataframes have equal rows and whatever order" in {

    Given("two different dataframes with the same scheme")

    val DataFrameGeneratorResult(df1, df2, keyFields) = generateDataFrames(equal = true)

    When("the dataframes are compared")
    val result = areEqual(df1, df2, keyFields)

    Then("the reult must be true")
    result shouldBe true
  }

}

