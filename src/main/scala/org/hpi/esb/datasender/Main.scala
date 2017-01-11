package org.hpi.esb.datasender
import java.nio.file.{Paths, Files}


object Main {

  def main(args: Array[String]) = {

    if (args.length != 2){
      println("Usage: java -jar DataSender.jar [data_file_path] [kafka_topic]")
      System.exit(1)
    }

    val dataInputPath = args(0)
    val kafkaTopic = args(1)

    if (! Files.exists(Paths.get(dataInputPath))) {
      println("The provided file path does not exist.")
      System.exit(1)
    }

    val dataProducer = new DataProducer(dataInputPath, kafkaTopic)
    dataProducer.execute()
  }
}
