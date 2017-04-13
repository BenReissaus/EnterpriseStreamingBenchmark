package org.hpi.esb.datavalidator.metrics

object CorrectnessMessages {

  def UNEQUAL_VALUES(topic: String, expectedValue: String, actualValue: String): String = {
    s"""
       |-------------------------
       |Topic $topic: The following two records differ:
       |Exp. $expectedValue
       |Act. $actualValue.
       |-------------------------""".stripMargin
  }

  def TOO_FEW_VALUES_CREATED(topic: String, valueType: String): String = s"Too few $valueType were created for topic $topic."

  def TOO_MANY_VALUES_CREATED(topic: String, valueType: String): String = s"Too many $valueType were created for topic $topic."
}

object Correctness {
  val header: List[String] = List("Correct")
}

class Correctness(private var isCorrect: Boolean = true) extends BenchmarkResult with ConstrainedMetric {

  def update(isCorrect: Boolean): Unit = {
    this.isCorrect &= isCorrect
  }

  override def getMeasuredResults: List[String] = List(fulfillsConstraint.toString)

  override def fulfillsConstraint: Boolean = isCorrect

  override def getResultsHeader: List[String] = Correctness.header
}
