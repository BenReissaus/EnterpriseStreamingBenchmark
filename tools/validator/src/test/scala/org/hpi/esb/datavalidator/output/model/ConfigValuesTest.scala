package org.hpi.esb.datavalidator.output.model

import org.scalatest.FunSpec
import ConfigValues._
import org.hpi.esb.commons.config.Configs.BenchmarkConfig

class ConfigValuesTest extends FunSpec {

  val sendingInterval = "100"
  val sendingIntervalTimeUnit = "SECONDS"
  val scaleFactor = "1"

  val exampleConfigValues = ConfigValues(sendingInterval, sendingIntervalTimeUnit, scaleFactor)

  describe("toList") {
    it("should return a list representation of the config values") {
      val configValuesList = exampleConfigValues.toList()
      val expectedList = List(sendingInterval, sendingIntervalTimeUnit, scaleFactor)

      assert(configValuesList == expectedList)
    }
  }

  describe("fromMap") {
    it("should return a ConfigValues object") {
      val valueMap = Map(
        SENDING_INTERVAL -> sendingInterval,
        SENDING_INTERVAL_TIMEUNIT -> sendingIntervalTimeUnit,
        SCALE_FACTOR -> scaleFactor
      )
      val configValuesFromMap = new ConfigValues(valueMap)

      assert(configValuesFromMap == exampleConfigValues)
    }
  }

  describe("get") {
    it("should return the most important config values") {
      val benchmarkConfig = BenchmarkConfig(
        topicPrefix = "ESB",
        benchmarkRun = 0,
        queries = List("Identity", "Statistics"),
        scaleFactor = scaleFactor.toInt,
        sendingInterval = sendingInterval.toInt,
        sendingIntervalTimeUnit = sendingIntervalTimeUnit
      )

      val configValues = ConfigValues.get(benchmarkConfig)
      assert(configValues == exampleConfigValues)
    }
  }

  describe("header") {
    val expectedHeader = List(SENDING_INTERVAL, SENDING_INTERVAL_TIMEUNIT, SCALE_FACTOR)
    assert(ConfigValues.header == expectedHeader)
  }
}
