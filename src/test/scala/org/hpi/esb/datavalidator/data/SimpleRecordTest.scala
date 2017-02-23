package org.hpi.esb.datavalidator.data

import org.scalatest.FunSuite

class SimpleRecordTest extends FunSuite {
  test("testCreate") {
    val r = SimpleRecord.deserialize("1000").get
    assert(r.value == 1000)
  }
}
