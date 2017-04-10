package org.hpi.esb.datavalidator.metrics

trait ConstrainedMetric extends Metric {

  def getSuccessMessage: String
  def getErrorMessage: String
  def fulfillsConstraint: Boolean

  override def getResultMessage: String = {
    if (fulfillsConstraint) getSuccessMessage else getErrorMessage
  }
}
