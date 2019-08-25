package com.gilcu2.dataframegenerator

import com.gilcu2.testUtil.SparkSessionTestWrapper
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class RandomDataFrameTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "RandomDataFrame"

  it should "generate a data frame with the numer of rows and specified" in {

    Given("the numbers of rows and columns")

    val nRows = 100
    val nKeys = 2
    val nOtherFields = 3

    When("generate the data frame")
    val df = RandomDataFrame.generate(nRows, nKeys, nOtherFields)

    Then("The parameter must be the specified")
    df.show()
    df.count shouldBe nRows
    df.columns.size shouldBe nKeys + nOtherFields
    val keyColumnsNames = df.columns.slice(0, nKeys)
    val keyColumns = keyColumnsNames.map(name => df.col(name))
    val dfKeys = df.select(keyColumns: _*)
    dfKeys.show()
    dfKeys.distinct().count shouldBe df.count
  }

}
