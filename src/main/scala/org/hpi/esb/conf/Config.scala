package org.hpi.esb.conf

import java.io.FileNotFoundException

import org.hpi.esb.util.Logging

import scalax.file.Path

package object Config extends Logging {

  def checkGreaterOrEqual(attributeName: String, attributeValue: Long, valueMinimum: Int): Boolean = {
    if (attributeValue < valueMinimum) {
      logger.error(s"Config invalid: sending interval is: $attributeName, but must be >= $valueMinimum.")
      return false
    }
    true
  }

  def checkAttributeHasValue(attributeName: String, attributeValue: String, conditionText: String = ""): Boolean = {
    if (attributeValue.trim.isEmpty) {
      logger.error(s"Config invalid: $attributeName must not be empty $conditionText")
      return false
    }
    true
  }

  case class Config(datasender: DataSenderConfig) {

    /**
      * Validation of configuration.
      * @return A bool that is true if the configuration is valid and false in case it is invalid.
      */
    def isValid: Boolean = datasender.isValid
  }

  case class DataSenderConfig(dataInputPath: String, sendingInterval: Long, columnDelimiter: String, numberOfThreads: Int,
    kafkaProducer: KafkaProducerConfig, dataModel: DataModelConfig) extends Logging {

    def isValid: Boolean = isDataInputPathValid && kafkaProducer.isValid && isSendingIntervalValid && isNumberOfThreadsValid && dataModel.isValid

    def isDataInputPathValid: Boolean = {
      if (!Path.fromString(dataInputPath).exists || !Path.fromString(dataInputPath).isFile) {
        logger.error(s"The provided file path (path:$dataInputPath) does not exist.")
        return false
      }
      true
    }

    def isSendingIntervalValid: Boolean = checkGreaterOrEqual("sending interval", sendingInterval, 0)
    def isNumberOfThreadsValid: Boolean = checkGreaterOrEqual("number of threads", numberOfThreads, 1)

  }

  case class KafkaProducerConfig(bootstrapServers: String, keySerializerClass: String, valueSerializerClass: String, acks: String, batchSize: Int) extends Logging {

    def isValid: Boolean = areServerAndSerializerAttributesValid && isAcksValid && isBatchSizeValid

    def areServerAndSerializerAttributesValid: Boolean =
      checkAttributeHasValue("bootstrap server list", bootstrapServers) && checkAttributeHasValue("key serializer class", keySerializerClass) && checkAttributeHasValue("value serializer class", valueSerializerClass)


    def isAcksValid: Boolean = {
      acks match {
        case "0" | "1" | "-1" | "all" => true
        case _ =>
          logger.error("Config invalid: acks must be either 0, 1, -1 or \"all\"")
          false
      }
    }

    def isBatchSizeValid: Boolean = checkGreaterOrEqual("batch size", batchSize, 0)

  }

  case class DataModelConfig(columns: List[String], columnStart: Option[Int], columnEnd: Option[Int]) extends Logging {

    def isValid: Boolean = {
      val colStart: Int = columnStart.getOrElse(0)
      val colEnd: Int = columnEnd.getOrElse(columns.size - 1)
      if (columns.isEmpty) {
        logger.error("You must specify at least one column.")
        return false
      }
      if (!((0 <= colStart) && (colStart <= colEnd) && (colEnd < columns.size))) {
        logger.error(s"Invalid config: column start and column end must be >= 0 and < size of column list; additionally, column start must be <= column end; \n "
          + s"column start: $colStart, \n column end: $colEnd, \n column list: $columns")
        return false
      }
      true
    }
  }
}

