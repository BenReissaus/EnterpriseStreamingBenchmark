package org.hpi.esb.datasender.config

import org.hpi.esb.commons.config.Configs
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
  val resultsPath = s"$dataSenderPath/results"
  val config: Config = getConfig()

  def resultFileName(currentTime: String): String = s"${Configs.benchmarkConfig.topicPrefix}_" +
    s"${Configs.benchmarkConfig.benchmarkRun}_$currentTime.csv"

  def resultFileNamePrefix(): String = s"${Configs.benchmarkConfig.topicPrefix}"

  private def getConfig(): Config = {
    if (!Path.fromString(userConfigPath).exists && Path.fromString(userConfigPath).isFile) {
      logger.error(s"The config file '$userConfigPath' does not exist.")
      sys.exit(1)
    }

    val config = loadConfigFromFiles[Config](Seq[java.io.File](new java.io.File(userConfigPath))) match {
      case Failure(f) => {
        logger.error(s"Invalid configuration for file $userConfigPath")
        logger.error(f.getMessage())
        sys.exit(1)
      }
      case Success(conf) => conf
    }

    if (!config.isValid) {
      logger.error(s"Invalid configuration:\n${config.toString}")
      sys.exit(1)
    }
    config
  }
}
