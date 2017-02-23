package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.mutable.ListBuffer
import org.hpi.esb.datavalidator.util.Logging

abstract class ResultValidation(inRecords: ListBuffer[ConsumerRecord[String, String]],
                                resultRecords: ListBuffer[ConsumerRecord[String, String]]) extends Logging {

  protected def recordsAreIncomplete(): Boolean = {
    if (inRecords.isEmpty) {
      logger.info(s"'In' records list is empty. Can not validate correctly.")
      return true
    }
    if (resultRecords.isEmpty) {
      logger.info(s"'Results' records list is empty. Can not validate correctly.")
      return true
    }
    false
  }

  def fulfillsRequirements(): Boolean
}
