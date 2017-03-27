package org.hpi.esb.datavalidator.validation

import org.scalatest.{BeforeAndAfter, FunSuite}

class IdentityValidationTest extends FunSuite with ValidationTestHelpers with BeforeAndAfter {

  val inTopic = "IN"
  val outTopic = "OUT"

  // (timestamp, "value")
  val inValues: List[(Long, String)] = List[(Long, String)](
    (1, "1"),
    (500, "2"),
    (1000, "10"),
    (1001, "20"),
    (1050, "30")
  )

  test("testExecute - successful") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val outValues = inValues
    val outRecords = createConsumerRecordList(outTopic, outValues)

    val identityValidation = new IdentityValidation(inRecords, outRecords)
    val validationResult = identityValidation.execute()
    assert(validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - incorrect results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val wrongOutValues: List[(Long, String)] = List[(Long, String)](
      (1, "999"),
      (500, "999"),
      (1000, "999"),
      (1001, "999"),
      (1050, "999")
    )
    val outRecords = createConsumerRecordList(outTopic, wrongOutValues)

    val identityValidation = new IdentityValidation(inRecords, outRecords)
    val validationResult = identityValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - too few results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val outValues: List[(Long, String)] = List[(Long, String)](
      (1, "1"),
      (500, "2")
    )
    val outRecords = createConsumerRecordList(outTopic, outValues)

    val identityValidation = new IdentityValidation(inRecords, outRecords)
    val validationResult = identityValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - too many results") {

    val inRecords = createConsumerRecordList(inTopic, inValues)

    val outValues: List[(Long, String)] = List[(Long, String)](
      (1, "1"),
      (500, "2"),
      (1000, "10"),
      (1001, "20"),
      (1050, "30"),
      (1070, "999"),
      (1090, "999")
    )
    val outRecords = createConsumerRecordList(outTopic, outValues)

    val identityValidation = new IdentityValidation(inRecords, outRecords)
    val validationResult = identityValidation.execute()
    assert(!validationResult.correctness.fulFillsConstraint)
  }

  test("testExecute - correct response time calculation") {
    val inValues: List[(Long, String)] = List[(Long, String)](
      (1, "1"), (500, "2"), (1000, "10"), (1001, "20"), (1050, "30"))

    val outValues: List[(Long, String)] = List[(Long, String)](
      (11, "1"), (510, "2"), (1010, "10"), (1011, "20"), (1060, "30"))

    val expectedResponseTimes = Array(10, 10, 10, 10, 10)

    val inRecords = createConsumerRecordList(inTopic, inValues)
    val statsRecords = createConsumerRecordList(outTopic, outValues)

    val identityValidation = new IdentityValidation(inRecords, statsRecords)
    val validationResult = identityValidation.execute()
    val responseTimeMetric = validationResult.responseTime
    assert(responseTimeMetric.getAllValues.sameElements(expectedResponseTimes))
  }
}
