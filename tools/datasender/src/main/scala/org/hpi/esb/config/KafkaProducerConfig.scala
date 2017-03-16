package org.hpi.esb.config

case class KafkaProducerConfig(bootstrapServers: Option[String],
                               keySerializerClass: Option[String],
                               valueSerializerClass: Option[String],
                               acks: Option[String],
                               batchSize: Option[Int]) extends Configurable {

  def isValid: Boolean = areServerAndSerializerAttributesValid && isAcksValid && isBatchSizeValid

  def areServerAndSerializerAttributesValid: Boolean =
    checkAttributeOptionHasValue("bootstrap server list", bootstrapServers) &&
      checkAttributeOptionHasValue("key serializer class", keySerializerClass) &&
      checkAttributeOptionHasValue("value serializer class", valueSerializerClass)

  def isAcksValid: Boolean = acks match {
    case Some("0" | "1" | "-1" | "all") => true
    case _ =>
      logger.error("Config invalid: acks must be either 0, 1, -1 or \"all\"")
      false
  }

  def isBatchSizeValid: Boolean = batchSize.isDefined && checkGreaterOrEqual("batch size", batchSize.get, 0)

  override def toString(): String = {
    val prefix = "datasender.kafkaProducer"

    s"""
$prefix.bootstrapServers = ${opToStr(bootstrapServers)}
$prefix.keySerializerClass = ${opToStr(keySerializerClass)}
$prefix.valueSerializerClass = ${opToStr(valueSerializerClass)}
$prefix.acks = ${opToStr(acks)}
$prefix.batchSize = ${opToStr(batchSize)}"""
  }
}

