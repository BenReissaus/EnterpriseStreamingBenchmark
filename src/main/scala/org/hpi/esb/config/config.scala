package org.hpi.esb

import org.hpi.esb.util.Logging
import scalax.file.Path

package object config extends Logging {

  def opToStr(v:Option[_], default: String = "") : String = v match {
    case Some(x) => x.toString
    case None if ! default.isEmpty => s"DEFAULT($default)"
    case _ => "NO VALUE"
  }

  // Set via cli parser
  case class CliConfig(
    dataInputPath:   Option[String] = None,
    sendingInterval: Option[Long] = None,
    verbose: Boolean = false
 )

  def checkGreaterOrEqual(attributeName: String, attributeValue: Long, valueMinimum: Int): Boolean = {
    val invalid = attributeValue < valueMinimum
    if (invalid) {
      logger.error(s"Config invalid: $attributeName but must be >= $valueMinimum.")
    }
    !invalid
  }

  def checkAttributeOptionHasValue(attributeName: String, attributeValue: Option[String], conditionText: String = ""): Boolean = {
    val invalid = attributeValue.isEmpty || attributeValue.get.trim.isEmpty
    if (invalid) {
      logger.error(s"Config invalid: $attributeName must be defined and not empty $conditionText")
    }
    !invalid
  }

  case class Config(datasender: DataSenderConfig) {

    /**
      * Validation of configuration.
      * @return A bool that is true if the configuration is valid and false in case it is invalid.
      */
    def isValid: Boolean = datasender.isValid
  }

  case class DataSenderConfig(
    dataInputPath: Option[String] ,
    sendingInterval: Option[Long],
    columnDelimiter: Option[String],
    numberOfThreads: Option[Int],
    kafkaProducer: KafkaProducerConfig,
    dataModel: DataModelConfig) extends Logging {

    def isValid: Boolean =
      isDataInputPathValid &&
      isSendingIntervalValid &&
      checkAttributeOptionHasValue("column delimiter",columnDelimiter) &&
      isNumberOfThreadsValid &&
      kafkaProducer.isValid &&
      dataModel.isValid

    override def toString() : String =
    {
      val prefix = "datasender"

      s"""
$prefix.dataInputPath = ${opToStr(dataInputPath)}
$prefix.sendingInterval = ${opToStr(sendingInterval)}
$prefix.columnDelimiter = ${opToStr(columnDelimiter)}
$prefix.numberOfThreads = ${opToStr(numberOfThreads)}
${kafkaProducer.toString}
${dataModel.toString}"""
    }

    def isDataInputPathValid: Boolean = {
      var valid = true
      if (dataInputPath.isEmpty)
      {
        logger.error("No data input path provided")
        valid = false
      }
      else if (!Path.fromString(dataInputPath.get).exists || !Path.fromString(dataInputPath.get).isFile) {
        logger.error(s"The provided file path (path:${dataInputPath.get}) does not exist.")
        valid = false
      }
      valid
    }

    def isSendingIntervalValid: Boolean = sendingInterval.isDefined && checkGreaterOrEqual("sending interval", sendingInterval.get, 0)
    def isNumberOfThreadsValid: Boolean = numberOfThreads.isDefined && checkGreaterOrEqual("number of threads", numberOfThreads.get, 1)
  }

  case class KafkaProducerConfig(
    bootstrapServers: Option[String],
    keySerializerClass: Option[String],
    valueSerializerClass: Option[String],
    acks: Option[String],
    batchSize: Option[Int]) extends Logging {

    def isValid: Boolean = areServerAndSerializerAttributesValid && isAcksValid && isBatchSizeValid

    override def toString() : String =
    {
      val prefix = "datasender.kafkaProducer"

s"""
$prefix.bootstrapServers = ${opToStr(bootstrapServers)}
$prefix.keySerializerClass = ${opToStr(keySerializerClass)}
$prefix.valueSerializerClass = ${opToStr(valueSerializerClass)}
$prefix.acks = ${opToStr(acks)}
$prefix.batchSize = ${opToStr(batchSize)}"""
    }

    def areServerAndSerializerAttributesValid: Boolean =
      bootstrapServers.isDefined &&
      checkAttributeOptionHasValue("bootstrap server list", bootstrapServers) &&
      keySerializerClass.isDefined &&
      checkAttributeOptionHasValue("key serializer class", keySerializerClass) &&
      valueSerializerClass.isDefined &&
      checkAttributeOptionHasValue("value serializer class", valueSerializerClass)

    def isAcksValid: Boolean = acks.isDefined && (acks.get match {
        case "0" | "1" | "-1" | "all" => true
        case _ =>
          logger.error("Config invalid: acks must be either 0, 1, -1 or \"all\"")
        false
    })

    def isBatchSizeValid: Boolean = batchSize.isDefined && checkGreaterOrEqual("batch size", batchSize.get, 0)
  }

  case class DataModelConfig(columns: Option[List[String]], columnStart: Option[Int], columnEnd: Option[Int]) extends Logging {

    def isValid: Boolean = {
      var valid = true

      val colStart: Int = columnStart.getOrElse(0)
      val colEnd: Int = columnEnd.getOrElse(columns.size - 1)
      if (columns.isEmpty || columns.get.isEmpty) {
        logger.error("You must specify at least one column.")
        valid = false
      } else if (!((0 <= colStart) && (colStart <= colEnd) && (colEnd < columns.size))) {
        logger.error(s"Invalid config: column start and column end must be >= 0 and < size of column list; additionally, " +
          s"column start must be <= column end; \n column start: $colStart, \n column end: $colEnd, \n column list: $columns")
        valid = false
      }
      valid
    }

    override def toString() : String =
    {
      val prefix = "datasender.dataModel"

s"""
$prefix.columns = ${opToStr(columns)}
$prefix.columnEnd = ${opToStr(columnEnd, "last column index")}
$prefix.columnStart = ${opToStr(columnStart, "0")}"""
    }
  }
}
