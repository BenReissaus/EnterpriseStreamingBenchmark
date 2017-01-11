package org.hpi.esb.datasender

import java.nio.file.{Files, Paths}


object Main {

  def main(args: Array[String]) = {

    if (args.length != 3) {
      println("Usage: java -jar DataSender.jar [data_file_path] [data_producer_period_micro_seconds] [kafka_topic]")
      System.exit(1)
    }

    val dataInputPath = args(0)
    val dataProducerPeriod = args(1).toLong
    val kafkaTopic = args(2)

    if (!Files.exists(Paths.get(dataInputPath))) {
      println("The provided file path does not exist.")
      System.exit(1)
    }

    val dataProducer = new DataProducer(dataInputPath, dataProducerPeriod, kafkaTopic)
    dataProducer.execute()
  }
}
