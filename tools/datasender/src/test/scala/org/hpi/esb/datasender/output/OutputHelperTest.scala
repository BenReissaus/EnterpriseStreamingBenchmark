package org.hpi.esb.datasender.output

import org.scalatest.FunSpec

class OutputHelperTest extends FunSpec {

  describe("concatMapValues") {
    it("should concatenate the values of two maps m1 and m2 where m1 has same keys as m2") {
      val m1 = Map("1" -> List("a"), "2" -> List("a"))
      val m2 = Map("1" -> List("b"), "2" -> List("b"))

      val expectedMap = Map("1" -> List("a", "b"), "2" -> List("a", "b"))
      val concatenatedMapValues = OutputHelper.concatMapValues(m1, m2)

      assert((expectedMap.toSet diff concatenatedMapValues.toSet).isEmpty)
    }

    it("should concatenate the values of two maps m1 and m2 where m1 has more keys than m2") {
      val m1 = Map("1" -> List("a"), "2" -> List("a"))
      val m2 = Map("1" -> List("b"))

      val expectedMap = Map("1" -> List("a", "b"), "2" -> List("a"))
      val concatenatedMapValues = OutputHelper.concatMapValues(m1, m2)

      assert((expectedMap.toSet diff concatenatedMapValues.toSet).isEmpty)
    }

    it("should concatenate the values of two maps m1 and m2 where m2 has more keys than m1") {
      val m1 = Map("1" -> List("a"))
      val m2 = Map("1" -> List("b"), "2" -> List("b"))

      val expectedMap = Map("1" -> List("a", "b"), "2" -> List("b"))
      val concatenatedMapValues = OutputHelper.concatMapValues(m1, m2)

      assert((expectedMap.toSet diff concatenatedMapValues.toSet).isEmpty)
    }

    it("should concatenate the values of two maps m1 and m2 where m1 and m2 have different keys") {
      val m1 = Map("1" -> List("a"))
      val m2 = Map("2" -> List("b"))

      val expectedMap = Map("1" -> List("a"), "2" -> List("b"))
      val concatenatedMapValues = OutputHelper.concatMapValues(m1, m2)

      assert((expectedMap.toSet diff concatenatedMapValues.toSet).isEmpty)
    }
  }

  describe("getSortedKeyValueLists") {
    it("should return a list of keys and a list of values that are sorted according to the keys") {
      val m = Map("A" -> "1", "B" -> "2", "C" -> "3", "D" -> "4")
      val (sortedKeys, sortedValues) = OutputHelper.getSortedKeyValueLists[String](m)
      val expectedKeys = List("A", "B", "C", "D")
      val expectedValues = List("1", "2", "3", "4")

      assert(sortedKeys == expectedKeys)
      assert(sortedValues == expectedValues)
    }
  }
}
