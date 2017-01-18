package org.hpi.esb.datasender

import java.nio.file.{Files, Paths}
import scopt.OptionParser

import org.hpi.esb.util.Logging

object Main {

  def main(args: Array[String]) = {

    val config = handleCliArguments(args)    
    val dataProducer = new DataProducer(config.producerConfig)
      
    if (config.verbose) Logging.setToDebug
    else Logging.setToInfo

    dataProducer.execute()
  }

  def handleCliArguments (args: Array[String]): DataSenderConfig =
  {
    val parser = new OptionParser[DataSenderConfig]("DataSender") {

      help("help").text("print this usage text")

      opt[String]('i', "inputPath")
        .required()
        .action ( (x,c) => c.copy(producerConfig = c.producerConfig.copy(dataInputPath = x))) 
        .text("file to be send line wise")

      opt[Long]('p', "producerPeriod")
        .required()
        .action ( (x,c) => c.copy(producerConfig = c.producerConfig.copy(dataProducerPeriod = x))) 
        .text("period of send operations")

      opt[String]('t', "kafkaTopic")
        .required()
        .action ( (x,c) => c.copy(producerConfig = c.producerConfig.copy(kafkaTopic = x))) 
        .text("target kafka topic")

      opt[Unit]('w', "wholeMessage")
        .optional()
        .action ( (x,c) => c.copy(producerConfig = c.producerConfig.copy(sendWholeMessage = true))) 
        .text("send whole message or just column 4")

      opt[Unit]('v', "verbose")
        .optional()
        .action ( (x,c) => c.copy(verbose = true) )
        .text("be verbose")

      checkConfig ( c =>
        if (!Files.exists(Paths.get(c.producerConfig.dataInputPath)))
          failure("The provided file path does not exist.")
        else success
      )
    }

    parser.parse(args, DataSenderConfig()) match {
      case (Some(config)) => config
      case None => sys.exit(1)
    }
  }
}


