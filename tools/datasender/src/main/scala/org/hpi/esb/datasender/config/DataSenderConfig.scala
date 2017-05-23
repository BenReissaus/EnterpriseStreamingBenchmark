package org.hpi.esb.datasender.config

import java.util.concurrent.TimeUnit

import DefaultValues._

import scala.util.{Failure, Success, Try}

case class DataSenderConfig(numberOfThreads: Option[Int], sendingInterval: Option[Int],
                            sendingIntervalTimeUnit: String = defaultSendingIntervalTimeUnit,
                            duration: Long = defaultDuration, durationTimeUnit: String = defaultDurationTimeUnit,
                            singleColumnMode: Boolean = false) extends Configurable {

  def isValid: Boolean =
    isValidSendingInterval(sendingInterval) &&
      isNumberOfThreadsValid && isTimeUnitValid(sendingIntervalTimeUnit) &&
  isTimeUnitValid(durationTimeUnit)

  def isNumberOfThreadsValid: Boolean = numberOfThreads.isDefined &&
    checkGreaterOrEqual("number of threads", numberOfThreads.get, 1)

  def isTimeUnitValid(timeUnit: String): Boolean = {
    val timeUnitEnum = Try(TimeUnit.valueOf(timeUnit))
    timeUnitEnum match {
      case Success(_) => true
      case Failure(_) => false
    }
  }

  def getDurationTimeUnit(): TimeUnit = TimeUnit.valueOf(durationTimeUnit)
  def getSendingIntervalTimeUnit(): TimeUnit = TimeUnit.valueOf(sendingIntervalTimeUnit)

  override def toString(): String = {
    val prefix = "datasender"
    s"""
      | $prefix.numberOfThreads = ${opToStr(numberOfThreads)}
      | $prefix.sendingInterval = ${opToStr(sendingInterval)}
      | $prefix.sendingIntervalTimeUnit = $sendingIntervalTimeUnit
      | $prefix.duration = $duration
      | $prefix.durationTimeUnit = $durationTimeUnit
      | $prefix.singleColumnMode = $singleColumnMode
    """.stripMargin
  }
}

object DefaultValues {
  val defaultDuration = 5
  val defaultDurationTimeUnit = TimeUnit.MINUTES.toString
  val defaultSendingIntervalTimeUnit = TimeUnit.MICROSECONDS.toString
  val defaultSingleColumn = false
}

