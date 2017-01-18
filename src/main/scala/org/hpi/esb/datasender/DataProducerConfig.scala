package org.hpi.esb.datasender

case class DataProducerConfig (
  dataInputPath: String = "",
  dataProducerPeriod: Long = 0,
  kafkaTopic: String = "",

  sendWholeMessage: Boolean = false
)
