package org.hpi.esb.datavalidator.output.model

import org.scalatest.FunSpec
import ResultValues._

class ResultValuesTest extends FunSpec {
  val query = "Identity"
  val correct = "true"
  val percentile = "1.3"
  val rtFulfilled = "true"
  val exampleResultValues = ResultValues(
    query = query,
    correct = correct.toBoolean,
    percentile = percentile.toDouble,
    rtFulfilled = rtFulfilled.toBoolean
  )

  describe("toList") {
    it("should return a list representation of the result values") {
      val exampleResultValuesList = exampleResultValues.toList()
      val expectedList = List(query, correct, percentile, rtFulfilled)
      assert(exampleResultValuesList == expectedList)
    }
  }

  describe("fromMap") {
    it("should return a ResultValues object") {
      val valueMap = Map(
        QUERY_COLUMN -> query,
        CORRECT_COLUMN -> correct,
        PERCENTILE_COLUMN -> percentile,
        RT_FULFILLED -> rtFulfilled
      )
      val resultValuesFromMap = new ResultValues(valueMap)

      assert(resultValuesFromMap == exampleResultValues)
    }
  }
}
