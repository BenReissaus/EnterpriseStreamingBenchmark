package org.hpi.esb.datavalidator

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.hpi.esb.datavalidator.consumer.Records
import org.hpi.esb.datavalidator.validation.StatisticsValidation
import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class ValidatorTest extends FunSuite {


  def createConsumerRecordList(topic: String, values: List[(Long, String)]): ListBuffer[ConsumerRecord[String, String]] = {
    val l = new ListBuffer[ConsumerRecord[String, String]]()
    values.foreach { case (t, v) => l.append(createConsumerRecord(topic, t, v)) }
    l
  }

  def createConsumerRecord(topic: String, timestamp: Long, value: String): ConsumerRecord[String, String] = {
    val partition = 0
    val offset = 0
    val checksum = 0
    val serializedKeySize = 0
    val serializedValueSize = 0
    val key = "0"

    new ConsumerRecord[String, String](topic, partition, offset, timestamp, TimestampType.CREATE_TIME, checksum, serializedKeySize, serializedValueSize, key, value)
  }

  test("testStatsValidate") {

    val inTopic = "IN"
    val statsTopic = "STATS"
    val windowSize = 1000
    val statsValidation = new StatisticsValidation(inTopic, statsTopic, windowSize)

    // (timestamp, "value")
    val inValues = List[(Long, String)](
      (1, "1"),
      (500, "2"),
      (1000, "10"),
      (1001, "20"),
      (1050, "30")
    )
    val inRecords = createConsumerRecordList(inTopic, inValues)

    // (timestamp, "min,max,sum,count,avg")
    val statsValues = List[(Long, String)](
      (2000, "1,2,3,2,1.5"),
      (3000, "10,30,60,3,20")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val records = new Records(List(inTopic, statsTopic))
    val inResults = records.getTopicResults(inTopic)
    val statsResults = records.getTopicResults(statsTopic)

    inRecords.foreach(inResults.append(_))
    statsRecords.foreach(statsResults.append(_))

    assert(statsValidation.execute(records))
  }
}
