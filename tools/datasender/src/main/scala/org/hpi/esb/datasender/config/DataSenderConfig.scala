package org.hpi.esb.datasender.config

import DefaultValues._
import java.util.concurrent.TimeUnit

import scala.util.{Failure, Success, Try}

case class DataSenderConfig(sendingInterval: Option[Int],
                            numberOfThreads: Option[Int],
                            singleColumnMode: Boolean = defaultSingleColumn,
                            timeUnit: String = defaultTimeUnit) extends Configurable {

  def isValid: Boolean =
    isValidSendingInterval(sendingInterval) &&
      isNumberOfThreadsValid && isTimeUnitValid

  def isNumberOfThreadsValid: Boolean = numberOfThreads.isDefined &&
    checkGreaterOrEqual("number of threads", numberOfThreads.get, 1)

  def isTimeUnitValid: Boolean = {
    val timeUnitEnum = Try(TimeUnit.valueOf(timeUnit))
    timeUnitEnum match {
      case Success(_) => true
      case Failure(_) => false
    }
  }

  override def toString(): String = {
    val prefix = "datasender"
    s"""
      | $prefix.sendingInterval = ${opToStr(sendingInterval)}
      | $prefix.timeUnit = $timeUnit
      | $prefix.numberOfThreads = ${opToStr(numberOfThreads)}
      | $prefix.singleColumnMode = $singleColumnMode
    """.stripMargin
  }
}

object DefaultValues {
  val defaultTimeUnit = "MICROSECONDS"
  val defaultSingleColumn = false
}
