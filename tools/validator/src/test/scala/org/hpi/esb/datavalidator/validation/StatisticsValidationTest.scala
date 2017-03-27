package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.util.Logging
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

class StatisticsValidationTest extends FunSuite with ValidationTestHelpers with BeforeAndAfter with Logging with MockitoSugar {

  val inTopic = "IN"
  val statsTopic = "STATS"
  val windowSize = 1000

  // (timestamp, "value")
  val inValues: List[(Long, String)] = List[(Long, String)](

    // first window
    (1, "1"), (500, "2"),
    // second window
    (1000, "10"), (1001, "20"), (1050, "30"))

  test("testExecute - correctness fulfilled") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    // (timestamp, "min,max,sum,count,avg")
    val statsValues = List[(Long, String)](
      (2000, "1,2,3,2,1.5"),
      (3000, "10,30,60,3,20")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - incorrect statistics results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val wrongStatsValues = List[(Long, String)](
      (2000, "999,999,999,999,999"),
      (3000, "10,30,60,3,20")
    )
    val statsRecords = createConsumerRecordList(statsTopic, wrongStatsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - too few statistics results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    // (timestamp, "min,max,sum,count,avg")
    val statsValues = List[(Long, String)](
      (1000, "1,2,3,2,1.5")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - too many statistics results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val statsValues = List[(Long, String)](
      (2000, "1,2,3,2,1.5"),
      (3000, "10,30,60,3,20"),
      (4000, "1234,1234,1234,1234,12345")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - empty 'IN' records list") {

    val inRecords = createConsumerRecordList(inTopic, List())

    val statsValues = List[(Long, String)](
      (2000, "1,2,3,2,1.5")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - unserializable sensor values") {
    val inValues: List[(Long, String)] = List[(Long, String)]((1, "notserializable"), (500, "notserializable"))
    val inRecords = createConsumerRecordList(inTopic, inValues)

    val statsValues = List[(Long, String)]((2000, "1,2,3,4,5"))
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - unserializable statistics values") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val statsValues = List[(Long, String)](
      (2000, "notserializable"),
      (3000, "notserializable"),
      (4000, "notserializable")
    )
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - correct response time calculation") {
    val inValues: List[(Long, String)] = List[(Long, String)](
      (1, "1"), (500, "2"),
      (1000, "10"), (1001, "20"), (1050, "30"))

    val statsValues: List[(Long, String)] = List[(Long, String)](
      (2000, "1,2,3,2,1.5"),
      (3000, "10,30,60,3,20"))

    val expectedResponseTimes = Array(1500, 1950)

    val inRecords = createConsumerRecordList(inTopic, inValues)
    val statsRecords = createConsumerRecordList(statsTopic, statsValues)

    val statsValidation = new StatisticsValidation(inRecords, statsRecords, windowSize)
    val validationResult = statsValidation.execute()
    val responseTimeMetric = validationResult.responseTime
    assert(responseTimeMetric.getAllValues.sameElements(expectedResponseTimes))
  }
}
