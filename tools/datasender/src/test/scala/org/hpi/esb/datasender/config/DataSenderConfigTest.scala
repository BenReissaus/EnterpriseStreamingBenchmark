package org.hpi.esb.datasender.config

import org.scalatest.FunSpec
import org.scalatest.mockito.MockitoSugar

class DataSenderConfigTest extends FunSpec with MockitoSugar {

  val numberOfThreads = Option(1)
  val singleColumnMode = false

  describe("isNumberOfThreadsValid") {
    it("should return false if number of threads is < 0") {
      val numberOfThreads = Option(-1)
      val config = DataSenderConfig(numberOfThreads, singleColumnMode)
      assert(!config.isNumberOfThreadsValid)
    }

    it("should return false if number of threads is 0") {
      val numberOfThreads = Option(0)
      val config = DataSenderConfig(numberOfThreads, singleColumnMode)
      assert(!config.isNumberOfThreadsValid)
    }

    it("should return true if number of threads is positive") {
      val numberOfThreads = Option(1)
      val config = DataSenderConfig(numberOfThreads, singleColumnMode)
      assert(config.isNumberOfThreadsValid)
    }
  }

}
