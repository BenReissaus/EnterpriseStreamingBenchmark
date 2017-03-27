package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hpi.esb.datavalidator.data.{Record, SimpleRecord, Statistics}
import org.hpi.esb.datavalidator.metrics.{CorrectnessMetric, ResponseTimeMetric}

import scala.collection.mutable.ListBuffer
import org.hpi.esb.datavalidator.util.Logging

import scala.util.{Failure, Success}

abstract class Validation(serializedInRecords: ListBuffer[ConsumerRecord[String, String]],
                          serializedOutRecords: ListBuffer[ConsumerRecord[String, String]]) extends Logging {

  val correctnessMetric = new CorrectnessMetric()
  val responseTimeMetric = new ResponseTimeMetric()

  def execute(): ValidationResult

  def deserializeSimpleRecords(records: ListBuffer[ConsumerRecord[String, String]]): ListBuffer[SimpleRecord] = {
    SimpleRecord.deserializeList(records).collect {
      case Success(x: SimpleRecord) => Some(x)
      case Failure(ex) => correctnessMetric.update(correct = false, details = ex.getMessage); None
    }.flatten
  }

  def deserializeStatisticRecords(records: ListBuffer[ConsumerRecord[String, String]]): ListBuffer[Statistics] = {
    Statistics.deserializeList(records).collect {
      case Success(x) => Some(x)
      case Failure(ex) => correctnessMetric.update(correct = false, details = ex.getMessage); None
    }.flatten
  }

  def getResponseTime(inRecord: Record, outRecord: Record): Long = {
    outRecord.timestamp - inRecord.timestamp
  }
}
