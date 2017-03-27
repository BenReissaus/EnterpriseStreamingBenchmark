package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.data.{SimpleRecord, Statistics}
import org.hpi.esb.datavalidator.metrics.CorrectnessMessages._
import org.hpi.esb.datavalidator.util.Logging

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class StatisticsValidation(serializedInRecords: ListBuffer[ConsumerRecord[String, String]],
                           serializedOutRecords: ListBuffer[ConsumerRecord[String, String]], windowSize: Long)
  extends Validation(serializedInRecords, serializedOutRecords) with Configurable with Logging {


  override def execute(): ValidationResult = {

    val simpleRecords = deserializeSimpleRecords(serializedInRecords)
    val outStatistics = deserializeStatisticRecords(serializedOutRecords)

    val goldStatistics = getGoldStatistics(simpleRecords)
    if (outStatistics.size != goldStatistics.size) {
      correctnessMetric.update(correct = false, details = UNEQUAL_LIST_SIZES_MESSAGE(goldStatistics.size, outStatistics.size))
      return new ValidationResult(correctnessMetric, responseTimeMetric)
    }

    // compare calculated with gold statistics
    val pairs = goldStatistics.zip(outStatistics)
    pairs.foreach { case (goldStatistic, outStatistic) =>

      responseTimeMetric.updateValue(getResponseTime(goldStatistic, outStatistic))

      if (goldStatistic != outStatistic) {
        correctnessMetric.update(correct = false, details = UNEQUAL_VALUES_MESSAGE(goldStatistic.prettyPrint, outStatistic.prettyPrint))
        return new ValidationResult(correctnessMetric, responseTimeMetric)
      }
    }

    new ValidationResult(correctnessMetric, responseTimeMetric)
  }

  private def getGoldStatistics(records: ListBuffer[SimpleRecord]): ListBuffer[Statistics] = {

    // this recursive function collects values per window and adds the calculated Statistics to 'statsList'
    @tailrec
    def getStatisticsWindow(leftRecords: ListBuffer[SimpleRecord], window: StatisticsWindow, statsList: ListBuffer[Statistics]): ListBuffer[Statistics] = {
      if (leftRecords.isEmpty)
        statsList
      else {
        val rest = window.takeRecords(leftRecords)
        statsList.append(window.stats)
        window.update()
        getStatisticsWindow(rest, window, statsList)
      }
    }

    getStatisticsWindow(records, getInitialWindow(records), new ListBuffer[Statistics]())
  }

  private def getInitialWindow(records: ListBuffer[SimpleRecord]): StatisticsWindow = {
    if (records.isEmpty) {
      new StatisticsWindow(Long.MaxValue, windowSize)
    } else {
      new StatisticsWindow(records.head.timestamp, windowSize)
    }
  }
}
