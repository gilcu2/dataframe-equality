package com.gilcu2.dataframegenerator

import com.gilcu2.testUtil.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class RandomDataFrameTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "RandomDataFrame"

  it should "generate two different data frame with the number of rows and columns specified" in {

    Given("the numbers of rows and columns")

    val nRows = 100
    val nKeys = 2
    val nOtherFields = 3

    When("generate the data frames")
    val (df, dfDifferent) = RandomDataFrame.generate(nRows, nKeys, nOtherFields)

    Then("The parameter of the data frames must be the specified")
    checkDataFrame(nRows, nKeys, nOtherFields, df)
    checkDataFrame(nRows, nKeys, nOtherFields, dfDifferent)

    And("The second dataframe must have the last field of the last column incremented")
    val lastRow = df.filter(_.getInt(0) == nRows).head
    val lastRowDifferent = dfDifferent.filter(_.getInt(0) == nRows).head
    lastRowDifferent.getInt(nKeys + nOtherFields) shouldBe lastRow.getInt(nKeys + nOtherFields) + 1
  }

  private def checkDataFrame(nRows: Int, nKeys: Int, nOtherFields: Int, df: DataFrame) = {
    df.count shouldBe nRows
    df.columns.size shouldBe nKeys + nOtherFields
    val keyColumnsNames = df.columns.slice(0, nKeys)
    val keyColumns = keyColumnsNames.map(name => df.col(name))
    val dfKeys = df.select(keyColumns: _*)
    dfKeys.distinct().count shouldBe df.count
  }
}
