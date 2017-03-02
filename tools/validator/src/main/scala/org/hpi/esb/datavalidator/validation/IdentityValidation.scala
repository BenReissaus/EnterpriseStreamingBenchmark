package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.util.Logging

import scala.collection.mutable.ListBuffer


class IdentityValidation(inRecords: ListBuffer[ConsumerRecord[String, String]],
                         resultRecords: ListBuffer[ConsumerRecord[String, String]])
  extends ResultValidation(inRecords, resultRecords) with Configurable with Logging {

  override def fulfillsRequirements(): Boolean = {

    if (inRecords.size != resultRecords.size) {
      logger.info(s"Invalid identity query result. Expected 'OUT' size: ${inRecords.size} Actual: ${resultRecords.size}")
      false
    }
    else {

      inRecords.zip(resultRecords)
        .foreach { case (r1, r2) =>
          if (r1.value() != r2.value()) {
            logger.info(s"Invalid identity query result: Expected value: ${r1.value()} but found value: ${r2.value()}.")
            return false
          }
        }
      logger.info(s"Valid identity query results. Both topics have size ${inRecords.size}.")
      true
    }
  }
}
