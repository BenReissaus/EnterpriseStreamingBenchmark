package org.hpi.esb.config

case class DataSenderConfig(sendingInterval: Option[Int],
                            numberOfThreads: Option[Int],
                            singleColumnMode: Boolean = false) extends Configurable {

  def isValid: Boolean =
    isValidSendingInterval(sendingInterval) &&
      isNumberOfThreadsValid

  def isNumberOfThreadsValid: Boolean = numberOfThreads.isDefined && checkGreaterOrEqual("number of threads", numberOfThreads.get, 1)

  override def toString(): String = {
    val prefix = "datasender"

    s"""
$prefix.sendingInterval = ${opToStr(sendingInterval)}
$prefix.numberOfThreads = ${opToStr(numberOfThreads)}"""
  }
}

