package org.hpi.esb.conf

package object configs {

  case class Config(datasender: DataSenderConfig)

  case class DataSenderConfig(dataInputPath: String, sendingInterval: Long, kafkaTopic: String, columnToBeSend: Int, columnDelimiter: String, numberOfThreads: Int, kafkaProducer: KafkaProducerConfig)

  case class KafkaProducerConfig(bootstrapServers: String, keySerializerClass: String, valueSerializerClass: String, acks: String, batchSize: String)

}

