package org.hpi.esb.datasender.config

import org.hpi.esb.commons.util.Logging
import pureconfig.loadConfigFromFiles
import scopt.OptionParser

import scala.util.{Failure, Success}
import scalax.file.Path

object ConfigHandler extends Logging {
  val projectPath = System.getProperty("user.dir")
  val dataSenderPath = s"$projectPath/tools/datasender"
  val configName = "datasender.conf"
  val userConfigPath = s"$dataSenderPath/$configName"

  def getConfig(args: Array[String]): Config = {
    val cliConfig = getCliConfig(args)
    val fileConfig = getFileConfig()
    mergeCliAndFileConfig(cliConfig, fileConfig)
  }

  def getFileConfig(): Config = {

    if (!Path.fromString(userConfigPath).exists && Path.fromString(userConfigPath).isFile) {
      logger.error(s"The config file '$userConfigPath' does not exist.")
      sys.exit(1)
    }

    val c = loadConfigFromFiles[Config](Seq[java.io.File](new java.io.File(userConfigPath))) match {
      case Failure(f) => {
        logger.error(s"Invalid configuration for file $userConfigPath")
        logger.error(f.getMessage())
        sys.exit(1)
      }
      case Success(conf) => conf
    }
    c
  }

  def mergeCliAndFileConfig(cliConfig: CliConfig, fileConfig: Config): Config = {
    var config = fileConfig


    if (cliConfig.dataInputPath.isDefined) {
      val newDataReaderConfig = config.dataReaderConfig.copy(dataInputPath = cliConfig.dataInputPath)
      config = config.copy(dataReaderConfig = newDataReaderConfig)
    }

    if (cliConfig.sendingInterval.isDefined) {
      val newDataSenderConfig = config.dataSenderConfig.copy(sendingInterval = cliConfig.sendingInterval)
      config = config.copy(dataSenderConfig = newDataSenderConfig)
    }

    if (cliConfig.verbose) {
      config = config.copy(verbose = true)
    }

    if (!config.isValid) {
      logger.error(s"Invalid configuration:\n${config.toString}")
      sys.exit(1)
    }
    config
  }

  def getCliConfig(args: Array[String]): CliConfig = {
    val parser = new OptionParser[CliConfig]("DataSender") {
      help("help").text("print this usage text")
      // TODO:Extend cli to override arbitrary parts of the configuration

      opt[String]('i', "inputPath")
        .optional()
        .action((x, c) => c.copy(dataInputPath = Some(x)))
        .text("file to be send line wise")

      opt[Int]('s', "sendingInterval")
        .optional()
        .action((x, c) => c.copy(sendingInterval = Some(x)))
        .text("interval of send operations")

      opt[Unit]('v', "verbose")
        .optional()
        .action((_, c) => c.copy(verbose = true))
        .text("be verbose")

      note(s"\nDefault user config file is expected at: $userConfigPath")

      checkConfig(c =>
        if (c.isValid) {
          success
        } else {
          failure("Please provide a correct path to an input file.")
        }
      )
    }

    parser.parse(args, CliConfig()) match {
      case (Some(config)) => config
      case None => sys.exit(1)
    }
  }

}
