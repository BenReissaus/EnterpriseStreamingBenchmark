package org.hpi.esb.commons.config

import java.util.concurrent.TimeUnit

import org.hpi.esb.commons.config.Configs.{BenchmarkConfig, BenchmarkConfigValidator}
import org.scalatest.FunSpec

class BenchmarkConfigValidatorTest extends FunSpec {
  val topicPrefix: String = "ESB"
  val benchmarkRun: Int = 1
  val queries: List[String] = List("Identity", "Statistics")
  val scaleFactor: Int = 1

  describe("isTimeUnitValid") {
    it("should return true when a correct string is passed") {
      val validTimeUnits = List("DAYS", "HOURS", "MICROSECONDS", "MILLISECONDS",
        "MINUTES", "NANOSECONDS", "SECONDS")

      validTimeUnits.foreach(timeUnit => {
        val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
          sendingIntervalTimeUnit = timeUnit, durationTimeUnit = timeUnit)
        assert(BenchmarkConfigValidator.isValid(config))
      })
    }

    it("should return false if a wrong string is passed") {
      val incorrectTimeUnit = "abcdef"
      val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
        sendingIntervalTimeUnit = incorrectTimeUnit, durationTimeUnit = incorrectTimeUnit)
      assert(!BenchmarkConfigValidator.isValid(config))

    }
  }
  describe("isValidSendingInterval") {
    it("should return false if sending interval is < 0") {
      val sendingInterval = -1
      val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
        sendingInterval = sendingInterval)
      assert(!BenchmarkConfigValidator.isValid(config))
    }

    it("should return false if sending interval  is 0") {
      val sendingInterval = 0
      val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
        sendingInterval = sendingInterval)
      assert(!BenchmarkConfigValidator.isValid(config))
    }

    it("should return true if sending interval is positive") {
      val sendingInterval = 1
      val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
        sendingInterval = sendingInterval)
      assert(!BenchmarkConfigValidator.isValid(config))
    }
  }

  describe("getSendingIntervalTimeUnit") {
    it("should return the correct sending interval time unit") {
      val sendingIntervalTimeUnit = TimeUnit.MINUTES.toString
      val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
        sendingIntervalTimeUnit = sendingIntervalTimeUnit)
      assert(config.getSendingIntervalTimeUnit() == TimeUnit.MINUTES)
    }
  }

  describe("getDurationTimeUnit") {
    it("should return the correct sending interval time unit") {
      val durationTimeUnit = TimeUnit.MINUTES.toString
      val config = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor,
        durationTimeUnit = durationTimeUnit)
      assert(config.getDurationTimeUnit() == TimeUnit.MINUTES)
    }
  }
}
