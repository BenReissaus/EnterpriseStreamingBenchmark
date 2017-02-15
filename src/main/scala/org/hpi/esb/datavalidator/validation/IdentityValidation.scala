package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.consumer.Records
import org.hpi.esb.datavalidator.util.Logging


class IdentityValidation(inTopic: String, outTopic: String) extends ResultValidation with Configurable with Logging {

  override def execute(sink: Records): Boolean = {
    val inRecords = sink.getTopicResults(inTopic)
    val outRecords = sink.getTopicResults(outTopic)

    if (inRecords.size != outRecords.size) {
      logger.info(s"Invalid identity query result. Expected 'OUT' size: ${inRecords.size} Actual: ${outRecords.size}")
      false
    }
    else {

      inRecords.zip(outRecords)
        .foreach { case (r1, r2) =>
          if (r1.value() != r2.value()) {
            logger.info(s"Invalid identity query result: Expected (time:${r1.timestamp()}, value:${r1.value()} Actual (time: ${r2.timestamp()}, value:${r2.value()}")
            return false
          }
        }
      logger.info(s"Valid identity query results.")
      true
    }
  }
}
