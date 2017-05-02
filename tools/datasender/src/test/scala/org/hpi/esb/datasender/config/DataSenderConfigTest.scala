package org.hpi.esb.datasender.config

import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

class DataSenderConfigTest extends FlatSpec with MockitoSugar {

  val numberOfThreads = Option(1)
  val sendingInterval = Option(1)
  val singleColumnMode = false

  "DataSenderConfig.isValid" should "return false if interval <= 0" in {
    val sendingInterval = Option(-1)
    val config = DataSenderConfig(sendingInterval, numberOfThreads, singleColumnMode)
    assert(!config.isValid)
  }

  it should "return false if number of threads <= 0" in {
    val numberOfThreads = Option(-1)
    val config = DataSenderConfig(sendingInterval, numberOfThreads, singleColumnMode)
    assert(!config.isValid)
  }
}
