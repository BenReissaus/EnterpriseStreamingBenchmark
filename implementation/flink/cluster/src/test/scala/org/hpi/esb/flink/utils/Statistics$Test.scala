package org.hpi.esb.flink.utils

import org.scalatest.{BeforeAndAfter, FunSuite}

class Statistics$Test extends FunSuite with BeforeAndAfter {

  test("testFold - Happy List") {
    val elements: Seq[Long] = Seq(1,2,3,8,9)
    val results = elements.foldLeft(new Statistics())(Statistics.fold)
    assert(results.min === 1)
    assert(results.max === 9)
    assert(results.avg === 4.6)
    assert(results.sum === 23)
    assert(results.count === 5)
  }

  test("testFold - Empty List") {
    val elements: Seq[Long] = Seq()
    val results = elements.foldLeft(new Statistics())(Statistics.fold)
    assert(results.min === Long.MaxValue)
    assert(results.max === Long.MinValue)
    assert(results.avg === 0)
    assert(results.sum === 0)
    assert(results.count === 0)
  }

  test("testFold - One element list") {
    val elements: Seq[Long] = Seq(0, 0, 0, 0)
    val results = elements.foldLeft(new Statistics())(Statistics.fold)
    assert(results.min === 0)
    assert(results.max === 0)
    assert(results.avg === 0)
    assert(results.sum === 0)
    assert(results.count === 4)
  }
}
