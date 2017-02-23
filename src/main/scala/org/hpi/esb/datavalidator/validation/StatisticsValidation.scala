package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.data.{SimpleRecord, Statistics}
import org.hpi.esb.datavalidator.util.Logging

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

class StatisticsValidation(inRecords: ListBuffer[ConsumerRecord[String, String]],
                           resultRecords: ListBuffer[ConsumerRecord[String, String]], windowSize: Long)
  extends ResultValidation(inRecords, resultRecords) with Configurable with Logging {

  val window = new StatisticsWindow(windowSize)

  override def fulfillsRequirements(): Boolean = {

    if (recordsAreIncomplete()) {
      return false
    }

    window.setInitialWindowEnd(inRecords.head.timestamp())
    val resultIter = resultRecords.toIterator

    inRecords.foreach(record => {

      if (!window.containsTimestamp(record.timestamp())) {
        if (!isValidWindow(resultIter)) {
          return false
        }
        window.update()
      }

      // TODO: use custom kafka deserializer interface instead
      val simpleRecord = SimpleRecord.deserialize(record.value()) match {
        case Success(v) => v
        case Failure(ex) => println(s"Invalid statistics query: The string '${record.value()}' is not a valid simple record serialization. ${ex.getMessage}."); return false
      }
      window.addValue(simpleRecord.value)
    })

    // evaluate last window
    if (!isValidWindow(resultIter)) {
      return false
    }

    if (resultIter.nonEmpty) {
      logger.info(s"Invalid statistics query: Too many statistics were calculated.")
      return false
    }

    logger.info(s"Valid statistics query results.")
    true
  }

  private def isValidWindow(resultIter: Iterator[ConsumerRecord[String, String]]): Boolean = {

    if (!resultIter.hasNext) {
      logger.info(s"Invalid statistics query: Too few statistics were calculated.")
      return false
    }
    val serializedStats = resultIter.next().value()

    val streamStats = Statistics.deserialize(serializedStats) match {
      case Success(stats) => stats
      case Failure(ex) => println(s"Invalid statistics query: The string '$serializedStats' is not a valid statistics object serialization. ${ex.getMessage}."); return false
    }
    if (!areCorrectStatisticResults(streamStats, window.stats)) {
      return false
    }

    true
  }

  private def areCorrectStatisticResults(streamStats: Statistics, goldStats: Statistics): Boolean = {
    if (window.stats != streamStats) {
      logger.info(s"Invalid statistics query: Incorrect Results. WindowEnd: ${window.windowEnd} GoldStats: ${window.stats.prettyPrint} StreamStats: ${streamStats.prettyPrint}")
      false
    }
    else {
      true
    }
  }
}
