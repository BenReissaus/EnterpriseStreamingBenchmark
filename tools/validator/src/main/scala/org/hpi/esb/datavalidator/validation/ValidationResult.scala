package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.metrics.{CorrectnessMetric, ResponseTimeMetric}

class ValidationResult(val correctness: CorrectnessMetric, val responseTime: ResponseTimeMetric) {

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
