package org.hpi.esb.datasender

case class DataSenderConfig (
  producerConfig: DataProducerConfig = new DataProducerConfig(),

  verbose: Boolean = false
)
