package org.hpi.esb.datasender.output.model

import org.scalatest.FunSpec
import ConfigValues._
import org.hpi.esb.commons.config.Configs.BenchmarkConfig
import org.hpi.esb.datasender.config.{Config, DataReaderConfig, DataSenderConfig, KafkaProducerConfig}

class ConfigValuesTest extends FunSpec {

  val batchSize = "1000"
  val bufferMemorySize = "1000"
  val lingerTime = "0"
  val readInRam = "true"
  val sendingInterval = "10"
  val sendingIntervalTimeUnit = "SECONDS"
  val scaleFactor = "1"

  val exampleConfigValues = ConfigValues(batchSize, bufferMemorySize, lingerTime, readInRam,
    sendingInterval, sendingIntervalTimeUnit, scaleFactor)

  describe("toList") {
    it("should return a list representation of the config values") {
      val configValuesList = exampleConfigValues.toList()
      val expectedList = List(batchSize, bufferMemorySize, lingerTime, readInRam,
        sendingInterval, sendingIntervalTimeUnit, scaleFactor)

      assert(configValuesList == expectedList)
    }
  }

  describe("fromMap") {
    it("should return a ConfigValues object") {
      val valueMap = Map(
        BATCH_SIZE -> batchSize,
        BUFFER_MEMORY_SIZE -> bufferMemorySize,
        LINGER_TIME -> lingerTime,
        READ_IN_RAM -> readInRam,
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
      val datasenderConfig = DataSenderConfig(numberOfThreads = Some(1))

      val kafkaProducerConfig = KafkaProducerConfig(
         bootstrapServers = Some("1234"),
        keySerializerClass = Some("keySerializerClass"),
        valueSerializerClass = Some("valueSerializerClass"),
        acks = Some("0"),
        batchSize = Some(batchSize.toInt),
        bufferMemorySize = bufferMemorySize.toLong,
        lingerTime = lingerTime.toInt
      )

      val dataReaderConfig = DataReaderConfig(
        columns = Some(List("A")),
        dataColumnStart = Some(0),
        columnDelimiter = Some(","),
        dataInputPath = Some("/path"),
        readInRam = readInRam.toBoolean
      )
      val config: Config = Config(datasenderConfig, dataReaderConfig, kafkaProducerConfig)

      val benchmarkConfig = BenchmarkConfig(
        topicPrefix = "ESB",
        benchmarkRun = 0,
        queries = List("Identity", "Statistics"),
        scaleFactor = scaleFactor.toInt,
        sendingInterval = sendingInterval.toInt,
        sendingIntervalTimeUnit = sendingIntervalTimeUnit
      )

      val configValues = ConfigValues.get(config, benchmarkConfig)

      assert(configValues == exampleConfigValues)
    }
  }

  describe("header") {
    val expectedHeader = List(BATCH_SIZE, BUFFER_MEMORY_SIZE, LINGER_TIME, READ_IN_RAM,
      SENDING_INTERVAL, SENDING_INTERVAL_TIMEUNIT, SCALE_FACTOR)
    assert(ConfigValues.header == expectedHeader)
  }
}
