package org.hpi.esb.datasender.config

case class DataSenderConfig(numberOfThreads: Option[Int], singleColumnMode: Boolean = false)
  extends Configurable {

  def isValid: Boolean = isNumberOfThreadsValid

  def isNumberOfThreadsValid: Boolean = numberOfThreads.isDefined &&
    checkGreaterOrEqual("number of threads", numberOfThreads.get, 1)

  override def toString(): String = {
    val prefix = "datasender"
    s"""
       | $prefix.numberOfThreads = ${opToStr(numberOfThreads)}
       | $prefix.singleColumnMode = $singleColumnMode
    """.stripMargin
  }
}
