package org.hpi.esb.datasender.output

import org.scalatest.FunSpec
import java.io.File

class BenchmarkRunResultMergerTest extends FunSpec {

  val benchmarkRunResultMerger = new BenchmarkRunResultMerger()
  describe("getAverageOfRunResultValues") {
    it("should return correctly calculated averages") {
      val results = List(Map("batch-size-avg" -> 100.toDouble.toString, "failedSends" -> 10.toDouble.toString),
        Map("batch-size-avg" -> 200.toString, "failedSends" -> 20.toString))
      val averageResults = benchmarkRunResultMerger.getAverageOfRunResultValues(results)
      val expectedAverageResults = Map("batch-size-avg" -> f"${150.toDouble}%1.0f", "failedSends" -> f"${15.toDouble}%1.0f")

      assert((averageResults.toSet diff expectedAverageResults.toSet).isEmpty)
    }
  }

  describe("filterSameRunFiles") {
    it("should return the correct files for the result merger") {
      val prefix = "ESB"
      val firstResultFile = new File(s"${prefix}_results1.csv")
      val secondResultFile = new File(s"${prefix}_results2.csv")
      val otherFile = new File("other.csv")
      val files = List(firstResultFile, secondResultFile, otherFile)
      val resultfiles = benchmarkRunResultMerger.filterSameRunFiles(files, prefix)

      assert(resultfiles.toSet.contains(firstResultFile))
      assert(resultfiles.toSet.contains(secondResultFile))
      assert(!resultfiles.toSet.contains(otherFile))
    }
  }
}
