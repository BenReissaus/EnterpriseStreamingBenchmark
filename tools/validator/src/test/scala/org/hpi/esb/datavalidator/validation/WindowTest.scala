package org.hpi.esb.datavalidator.validation

import org.scalatest.{BeforeAndAfter, FunSuite}

class WindowTest extends FunSuite with BeforeAndAfter {

  val window = new Window(windowSize = 1000)

  test("testSetInitialWindowEnd") {

    var timestamps = List(0, 1, 100, 999)

    timestamps.foreach(t => {
      window.setInitialWindowEnd(firstElementTimestamp = t)
      assert(window.windowEnd == 1000)
    })

    timestamps = List(1000, 1001)

    timestamps.foreach(t => {
      window.setInitialWindowEnd(firstElementTimestamp = t)
      assert(window.windowEnd == 2000)
    })
  }

  test("testContainsTimestamp") {

    window.setInitialWindowEnd(firstElementTimestamp = 1)

    val containedValues = List(0,1,100,999)
    assert(containedValues.forall(window.containsTimestamp(_)))

    val notContainedValues = List(1000, 1001)
    assert(notContainedValues.forall(!window.containsTimestamp(_)))
  }

  test("testUpdateWindowEnd") {

    window.setInitialWindowEnd(firstElementTimestamp = 1)
    window.update()

    assert(window.windowEnd == 2000)
  }
}
