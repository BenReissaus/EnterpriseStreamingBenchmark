package org.hpi.esb.datavalidator.metrics

object CorrectnessMessages {

  def UNEQUAL_LIST_SIZES_MESSAGE(expectedSize: Long, actualSize: Long): String =
    s"Expected result size: $expectedSize Actual: $actualSize"

  def UNEQUAL_VALUES_MESSAGE(expectedValue: String, actualValue: String): String =
    s"Expected value: $expectedValue Actual: $actualValue."

}

class CorrectnessMetric(var correct: Boolean = true, var details: String = "") extends ConstrainedMetric {
  override def getSuccessMessage: String = s"All values were correctly calculated. $details"

  override def getErrorMessage: String = s"Not all values were correctly calculated. The following incidents occurred: $details"

  override def fulFillsConstraint: Boolean = correct

  def update(correct: Boolean, details: String): Unit = {
    this.correct &= correct
    this.details += s"\n$details"
  }
}
