package org.hpi.esb.datavalidator.data

import org.scalatest.FunSuite

class SimpleRecordTest extends FunSuite {
  test("testCreate - Successful") {
    val r = SimpleRecord.deserialize("1000", 1)
    assert(r.value == 1000)
  }

  test("testCreate - Unsuccessful") {
    assertThrows[IllegalArgumentException] {
      SimpleRecord.deserialize("incorrect string", 1)
    }
  }
}
