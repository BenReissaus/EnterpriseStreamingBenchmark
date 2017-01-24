package org.hpi.esb.datasender

import java.io.FileNotFoundException
import org.hpi.esb.conf.configs.Config
import org.hpi.esb.util.Logging
import scala.util.{Failure, Success}
import pureconfig.loadConfig
import scalax.file.Path

object Main extends Logging {

  val config = loadConfig[Config] match {
    case Failure(f) => throw f
    case Success(conf) => {
      if (!Path.fromString(conf.datasender.dataInputPath).exists ||
        !Path.fromString(conf.datasender.dataInputPath).isFile) {
        throw new FileNotFoundException(s"The provided file path (path:${conf.datasender.dataInputPath}) does not exist.")
        System.exit(1)
      }
      conf
    }
  }

  val verboseOptionShort:String = "-v"
  val verboseOptionLong:String = "--verbose"
  val usage = s"""
    Usage: sbt run | sbt \"run $verboseOptionShort\"  sbt \"run $verboseOptionLong\"
  """

  def main(args: Array[String]) = {

    Logging.setToInfo
    if (args.length > 0) {
      if (args.length == 1 && (args(0).trim.equalsIgnoreCase(verboseOptionShort) ||
        args(0).trim.equalsIgnoreCase(verboseOptionLong))) {
        Logging.setToDebug
        logger.info("DEBUG/VERBOSE mode switched on")
      } else {
        logger.warn(usage)
        logger.warn("ignoring arguments")
      }
    }

    val dataProducer = new DataProducer(config.datasender)
    dataProducer.execute
  }
}


