package org.hpi.esb.datavalidator.metrics

trait ConstrainedMetric extends Metric {

  def getSuccessMessage: String
  def getErrorMessage: String
  def fulFillsConstraint: Boolean

  override def getResultMessage: String = {
    if (fulFillsConstraint) getSuccessMessage else getErrorMessage
  }
}
