package org.hpi.esb.config


case class Config(dataSenderConfig: DataSenderConfig, dataReaderConfig: DataReaderConfig,
                  kafkaProducerConfig: KafkaProducerConfig, verbose: Boolean = false) {
  def isValid: Boolean = dataSenderConfig.isValid && dataReaderConfig.isValid & kafkaProducerConfig.isValid
}




