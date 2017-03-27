package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.metrics.CorrectnessMessages._
import org.hpi.esb.datavalidator.util.Logging

import scala.collection.mutable.ListBuffer


class IdentityValidation(serializedInRecords: ListBuffer[ConsumerRecord[String, String]],
                         serializedOutRecords: ListBuffer[ConsumerRecord[String, String]])
  extends Validation(serializedInRecords, serializedOutRecords) with Configurable with Logging {

  override def execute(): ValidationResult = {

    val inRecords = deserializeSimpleRecords(serializedInRecords)
    val outRecords = deserializeSimpleRecords(serializedOutRecords)

    if (inRecords.size != outRecords.size) {
      correctnessMetric.update(correct = false, details = UNEQUAL_LIST_SIZES_MESSAGE(outRecords.size, inRecords.size))
      return new ValidationResult(correctnessMetric, responseTimeMetric)
    }

    // compare 'IN' records with 'OUT" records
    val pairs = inRecords.zip(outRecords)
    pairs.foreach { case (inRecord, outRecord) =>

      responseTimeMetric.updateValue(getResponseTime(inRecord, outRecord))

      if (inRecord.value != outRecord.value) {
        correctnessMetric.update(correct = false, details = UNEQUAL_VALUES_MESSAGE(inRecord.value.toString, outRecord.value.toString))
        return new ValidationResult(correctnessMetric, responseTimeMetric)
      }
    }

    correctnessMetric.update(correct = true, details = s"Both topics have size ${serializedInRecords.size}.")
    new ValidationResult(correctnessMetric, responseTimeMetric)
  }

}
