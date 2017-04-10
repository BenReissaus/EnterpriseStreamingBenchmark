package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.metrics.{CorrectnessMetric, ResponseTimeMetric}

class ValidationResult(val correctness: CorrectnessMetric = new CorrectnessMetric(),
                       val responseTime: ResponseTimeMetric = new ResponseTimeMetric()) {

  def updateCorrectness(isCorrect: Boolean, details: String): Unit = {
    correctness.update(isCorrect = isCorrect, details = details)
  }

  def updateResponseTime(value: Long): Unit = {
    responseTime.updateValue(value)
  }

  def fulfillsConstraints(): Boolean = correctness.fulfillsConstraint &&
  responseTime.fulfillsConstraint


  override def toString: String = {
    s"""
       |-----------------------------------------------
       |Correctness:
       |${correctness.getResultMessage}
       |
       |ResponseTime:
       |${responseTime.getResultMessage}
       |-----------------------------------------------
     """.stripMargin
  }
}
