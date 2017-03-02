package org.hpi.esb.datavalidator.validation

import org.scalatest.{BeforeAndAfter, FunSuite}

class StatisticsValidationTest extends FunSuite with ValidationTestHelpers with BeforeAndAfter {

  val inTopic = "IN"
  val statsTopic = "STATS"
  val windowSize = 1000

  // (timestamp, "value")
  val inValues:List[(Long, String)] = List[(Long, String)](
    // first window
    (1, "1"),
    (500, "2"),

    // second window
    (1000, "10"),
    (1001, "20"),
    (1050, "30")
  )

  test("testExecute - Successful") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    // (timestamp, "min,max,sum,count,avg")
    val statsValues = List[(Long, String)](
      (2000, "1,2,3,2,1.5"),
      (3000, "10,30,60,3,20")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    assert(statsValidation.fulfillsRequirements())
  }

  test("testExecute - Incorrect Results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val wrongStatsValues = List[(Long, String)](
      (2000, "999,999,999,999,999"),
      (3000, "10,30,60,3,20")
    )
    val statsRecords = createConsumerRecordList(statsTopic, wrongStatsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    assert(!statsValidation.fulfillsRequirements())
  }

  test("testExecute - Too few statistics") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    // (timestamp, "min,max,sum,count,avg")
    val statsValues = List[(Long, String)](
      (1000, "1,2,3,2,1.5")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    assert(!statsValidation.fulfillsRequirements())
  }

  test("testExecute - Too many statistics") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val statsValues = List[(Long, String)](
      (2000, "1,2,3,2,1.5"),
      (3000, "10,30,60,3,20"),
      (4000, "1234,1234,1234,1234,12345")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    assert(!statsValidation.fulfillsRequirements())
  }
}
