package org.hpi.esb.datavalidator.output.model

import org.hpi.esb.commons.output.Util._

object ResultValues {
  val QUERY_COLUMN = "Query"
  val CORRECT_COLUMN = "Correct"
  val PERCENTILE_COLUMN = "RT-90%ile"
  val RT_FULFILLED = "RT-Fulfilled"
  val header = List(QUERY_COLUMN, CORRECT_COLUMN, PERCENTILE_COLUMN, RT_FULFILLED)
}

import org.hpi.esb.datavalidator.output.model.ResultValues._

case class ResultValues(query: String, correct: Boolean, percentile: Double, rtFulfilled: Boolean = false) {

  def this(m: Map[String, String]) = this(m(QUERY_COLUMN), m(CORRECT_COLUMN).toBoolean,
    m(PERCENTILE_COLUMN).toDouble, m(RT_FULFILLED).toBoolean)

  def toList(): List[String] = {
    val formattedPercentile = format(round(percentile, precision = 2))
    List(query, correct.toString, formattedPercentile, rtFulfilled.toString)
  }
}
