package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.consumer.Records
import org.hpi.esb.datavalidator.data.{SimpleRecord, Statistics}
import org.hpi.esb.datavalidator.util.Logging

class StatisticsValidation(inTopic: String, statsTopic: String, windowSize: Long) extends ResultValidation with Configurable with Logging {

  override def execute(results: Records): Boolean = {
    val source = results.getTopicResults(inTopic)
    val sink = results.getTopicResults(statsTopic)
    val sinkLength = sink.length

    val window = new Window(windowSize, source.head.timestamp())
    var statsCount = 0
    var goldStats = new Statistics()

    for (record <- source) {

      if (!window.contains(record.timestamp())) {

        if (statsCount >= sinkLength) {
          logger.info(s"Invalid statistics query: Too few statistics were calculated.")
          return false
        }

        // validate last window
        val streamStats = Statistics.create(sink(statsCount).value())
        if (!isValidStatisticsWindow(goldStats, streamStats)) {
          logger.info(s"Invalid statistics query. WindowEnd: ${window.windowEnd} GoldStats: ${goldStats.prettyPrint} StreamStats: ${streamStats.prettyPrint}")
          return false
        }

        // prepare next window
        statsCount += 1
        goldStats = new Statistics()
        window.updateWindowEnd()
      }

      // add next value to statistics
      goldStats = goldStats.addValue(SimpleRecord.create(record.value()))
    }
    logger.info(s"Valid statistics query results.")
    true
  }

  def isValidStatisticsWindow(realStats: Statistics, streamStats: Statistics): Boolean = {
    streamStats == realStats
  }
}

