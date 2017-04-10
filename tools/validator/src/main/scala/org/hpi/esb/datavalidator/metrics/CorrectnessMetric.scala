package org.hpi.esb.datavalidator.metrics

object CorrectnessMessages {

  def UNEQUAL_VALUES(expectedValue: String, actualValue: String): String =
    s"Expected value: $expectedValue Actual: $actualValue."

  def TOO_FEW_VALUES_CREATED(valueType: String): String = s"Too few $valueType were created."
  def TOO_MANY_VALUES_CREATED(valueType: String): String = s"Too many $valueType were created."
}

class CorrectnessMetric(private var isCorrect: Boolean = true, private var details: String = "") extends ConstrainedMetric {
  override def getSuccessMessage: String = s"All values were correctly calculated."

  override def getErrorMessage: String = s"Not all values were correctly calculated. The following incidents occurred: $details"

  override def fulfillsConstraint: Boolean = isCorrect

  def update(isCorrect: Boolean, details: String): Unit = {
    this.details = s"\n$details"
    this.isCorrect &= isCorrect
  }
}
