package org.hpi.esb.datasender

import org.hpi.esb.config.{Config, CliConfig, DataSenderConfig}
import org.hpi.esb.util.Logging

import pureconfig.loadConfigFromFiles
import scopt.OptionParser

import scala.util.{Failure, Success}
import java.io.FileNotFoundException
import scalax.file.Path


object Main extends Logging {

  val RelativeDefaultUserConfigPath = "/datasender.conf"
  val userConfigPath = System.getProperty("user.dir") + RelativeDefaultUserConfigPath

  def main(args: Array[String]) : Unit = {

    val cliConfig = getCliConfig(args)
    val fileConfig = getFileConfig()

    val configToUse = mergeCliAndFileConfig(cliConfig, fileConfig)

    if (cliConfig.verbose)
    {
      Logging.setToDebug
      logger.info("DEBUG/VERBOSE mode switched on")
    }
    else
    {
      Logging.setToInfo
    }

    if (! configToUse.isValid)
    {
      logger.error(s"Invalid configuration:\n${configToUse.toString}")
      sys.exit(1)
    }

    val dataProducer = new DataProducer(configToUse.datasender)
    dataProducer.execute
  }

  def isValidFile(path: String) : Boolean = Path.fromString(path).exists && Path.fromString(path).isFile

  def getConfigFile() : java.io.File =
  {
    if (! isValidFile(userConfigPath))
    {
      logger.warn(s"$userConfigPath is not a valid file")
      throw new java.io.FileNotFoundException(s"No valid configuration found: $userConfigPath is not a valid file")
    }

    logger.info(s"Using config file $userConfigPath")
    new java.io.File(userConfigPath)
  }

  def getFileConfig() : Config =
  {
    val configFile = getConfigFile()

    val c = loadConfigFromFiles[Config](Seq[java.io.File](configFile)) match {
     case Failure(f) => {
        logger.error(s"Invalid configuration: ${configFile.getCanonicalPath()}")
        logger.error(f.getMessage())
        sys.exit(1)
     }
     case Success(conf) => {
        if (conf.datasender.dataInputPath.isDefined &&
            ! isValidFile(conf.datasender.dataInputPath.get)) {
          throw new FileNotFoundException(s"The provided file path (path:${conf.datasender.dataInputPath.get}) does not exist.")
        }
        conf
      }
    }

    c
  }

  def mergeCliAndFileConfig(cliConfig: CliConfig, fileConfig: Config) : Config =
  {
    var config = fileConfig

    if (cliConfig.dataInputPath.isDefined)
    {
      config = config.copy( datasender = config.datasender.copy(dataInputPath = cliConfig.dataInputPath) )    }

    if (cliConfig.sendingInterval.isDefined)
    {
      config = config.copy( datasender = config.datasender.copy(sendingInterval = cliConfig.sendingInterval) )
    }

    config
  }

  def getCliConfig(args: Array[String]): CliConfig =
  {
    val parser = new OptionParser[CliConfig]("DataSender") {
      help("help").text("print this usage text")
      // TODO:Extend cli to override arbitrary parts of the configuration

      opt[String]('i', "inputPath")
        .optional()
        .action ( (x,c) => c.copy(dataInputPath = Some(x)))
        .text("file to be send line wise")

      opt[Long]('s', "sendingInterval")
        .optional()
        .action ( (x,c) => c.copy(sendingInterval = Some(x)))
        .text("interval of send operations")

      opt[Unit]('v', "verbose")
        .optional()
        .action ( (_,c) => c.copy(verbose = true) )
        .text("be verbose")

      note(s"\nDefault user config file is expected at: $userConfigPath")

      checkConfig ( c =>
        if (c.dataInputPath.isDefined && ! isValidFile(c.dataInputPath.get))
        {
          failure("The provided input file does not exist.")
        }
        else
        {
          success
        }
      )
    }

    parser.parse(args, CliConfig()) match {
      case (Some(config)) => config
      case None => sys.exit(1)
    }
  }
}
