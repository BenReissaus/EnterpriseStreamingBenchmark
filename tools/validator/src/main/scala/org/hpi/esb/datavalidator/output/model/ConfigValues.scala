package org.hpi.esb.datavalidator.output.model

import org.hpi.esb.commons.config.Configs.BenchmarkConfig

object ConfigValues {
  val SENDING_INTERVAL = "sendingInterval"
  val SENDING_INTERVAL_TIMEUNIT = "sendingIntervalTimeUnit"
  val SCALE_FACTOR = "scaleFactor"

  val header = List(SENDING_INTERVAL, SENDING_INTERVAL_TIMEUNIT, SCALE_FACTOR)

  def get(benchmarkConfig: BenchmarkConfig): ConfigValues = {
    ConfigValues(benchmarkConfig.sendingInterval.toString,
      benchmarkConfig.sendingIntervalTimeUnit,
      benchmarkConfig.scaleFactor.toString)
  }
}

import org.hpi.esb.datavalidator.output.model.ConfigValues._

case class ConfigValues(sendingInterval: String, sendingIntervalUnit: String, scaleFactor: String) {

  def this(m: Map[String, String]) = this(m(SENDING_INTERVAL), m(SENDING_INTERVAL_TIMEUNIT), m(SCALE_FACTOR))

  def toList(): List[String] = {
    List(sendingInterval.toString, sendingIntervalUnit, scaleFactor)
  }
}

