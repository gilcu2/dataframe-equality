package com.gilcu2.experiments

import com.gilcu2.testUtil.SparkSessionTestWrapper
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PerformanceTest extends FlatSpec with Matchers with GivenWhenThen
  with SparkSessionTestWrapper with LazyLogging {

  behavior of "Performance"

  implicit val logger1 = logger

  it should "measure the time of the 6 algorithms" in {

    Given("the parameters of the test data")
    val sizes = Seq(10, 20, 30)
    val nKeys = 2
    val nOtherFields = 3

    When("the performance experiments are done")
    val results = Performance.measureTimesPerAlgorithm(sizes, nKeys, nOtherFields)

    Then("the results must have the time for each algorithm and data")
    results.byDirectComparisonEqual.size shouldBe 3

  }

}
