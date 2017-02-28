package org.hpi.esb.datasender

import org.hpi.esb.config.Config
import org.hpi.esb.util.Logging
import pureconfig.loadConfig
import scala.util.{Failure, Success}

object Main extends Logging {

  val config: Config = loadConfig[Config] match {
    case Failure(f) => throw f
    case Success(conf) =>
      if (!conf.isValid) {
        throw new RuntimeException("Config validation failed.")
        System.exit(1)
      }
      conf
  }

  val verboseOptionShort: String = "-v"
  val verboseOptionLong: String = "--verbose"
  val usage = s"""
    Usage: sbt run | sbt \"run $verboseOptionShort\"  sbt \"run $verboseOptionLong\"
  """

  def main(args: Array[String]): Unit = {

    Logging.setToInfo()
    if (args.length > 0) {
      if (args.length == 1 && (args(0).trim.equalsIgnoreCase(verboseOptionShort) ||
        args(0).trim.equalsIgnoreCase(verboseOptionLong))) {
        Logging.setToDebug()
        logger.info("DEBUG/VERBOSE mode switched on")
      } else {
        logger.warn(usage)
        logger.warn("ignoring arguments")
      }
    }

    val dataProducer = new DataProducer(config.datasender)
    dataProducer.execute()
  }
}


