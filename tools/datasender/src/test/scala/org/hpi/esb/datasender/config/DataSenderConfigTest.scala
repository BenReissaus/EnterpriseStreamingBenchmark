package org.hpi.esb.datasender.config

import java.util.concurrent.TimeUnit

import org.scalatest.FunSpec
import org.scalatest.mockito.MockitoSugar

class DataSenderConfigTest extends FunSpec with MockitoSugar {

  val numberOfThreads = Option(1)
  val sendingInterval = Option(1)
  val singleColumnMode = false

  describe("isNumberOfThreadsValid") {
    it("should return false if number of threads is < 0") {
      val numberOfThreads = Option(-1)
      val config = DataSenderConfig(numberOfThreads, sendingInterval, singleColumnMode = singleColumnMode)
      assert(!config.isNumberOfThreadsValid)
    }

    it("should return false if number of threads is 0") {
      val numberOfThreads = Option(0)
      val config = DataSenderConfig(numberOfThreads, sendingInterval, singleColumnMode = singleColumnMode)
      assert(!config.isNumberOfThreadsValid)
    }

    it("should return true if number of threads is positive") {
      val numberOfThreads = Option(1)
      val config = DataSenderConfig(numberOfThreads, sendingInterval, singleColumnMode = singleColumnMode)
      assert(config.isNumberOfThreadsValid)
    }
  }

  describe("isTimeUnitValid") {
    it("should return true when a correct string is passed") {
      val validTimeUnits = List("DAYS", "HOURS", "MICROSECONDS", "MILLISECONDS",
        "MINUTES", "NANOSECONDS", "SECONDS")

      validTimeUnits.foreach(timeUnit => {
        val config = DataSenderConfig(numberOfThreads, sendingInterval, timeUnit, singleColumnMode = singleColumnMode)
        assert(config.isTimeUnitValid(timeUnit))
      })
    }

    it("should return false if a wrong string is passed") {
      val incorrectTimeUnit = "abcdef"
      val config = DataSenderConfig(numberOfThreads, sendingInterval, incorrectTimeUnit, singleColumnMode = singleColumnMode)
      assert(!config.isTimeUnitValid(incorrectTimeUnit))
    }
  }
  describe("isValidSendingInterval") {
    it("should return false if sending interval is < 0") {
      val sendingInterval = Option(-1)
      val config = DataSenderConfig(numberOfThreads, sendingInterval, singleColumnMode = singleColumnMode)
      assert(!config.isValidSendingInterval(sendingInterval))
    }

    it("should return false if sending interval  is 0") {
      val sendingInterval = Option(0)
      val config = DataSenderConfig(numberOfThreads, sendingInterval, singleColumnMode = singleColumnMode)
      assert(!config.isValidSendingInterval(sendingInterval))
    }

    it("should return true if sending interval is positive") {
      val sendingInterval = Option(1)
      val config = DataSenderConfig(numberOfThreads, sendingInterval, singleColumnMode = singleColumnMode)
      assert(config.isValidSendingInterval(sendingInterval))
    }
  }

  describe("getSendingIntervalTimeUnit") {
    it("should return the correct sending interval time unit") {
      val sendingIntervalTimeUnit = TimeUnit.MINUTES.toString
      val config = DataSenderConfig(numberOfThreads, sendingInterval, sendingIntervalTimeUnit)
      assert(config.getSendingIntervalTimeUnit() == TimeUnit.MINUTES)
    }
  }

  describe("getDurationTimeUnit") {
    it("should return the correct sending interval time unit") {
      val durationTimeUnit = TimeUnit.MINUTES.toString
      val config = DataSenderConfig(numberOfThreads, sendingInterval, durationTimeUnit = durationTimeUnit)
      assert(config.getDurationTimeUnit() == TimeUnit.MINUTES)
    }
  }
}
