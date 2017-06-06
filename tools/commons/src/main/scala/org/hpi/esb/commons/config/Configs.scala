package org.hpi.esb.commons.config

import java.io.File
import java.util.concurrent.TimeUnit

import org.hpi.esb.commons.util.Logging
import pureconfig.loadConfigFromFiles

import scala.util.{Failure, Success, Try}

object Configs extends Logging {

  private val relativeConfigPath = "/tools/commons/commons.conf"
  private val configPath: String = System.getProperty("user.dir") + relativeConfigPath
  val benchmarkConfig: BenchmarkConfig = getConfig(configPath)

  def getConfig(configPath: String): BenchmarkConfig = {
    val config = loadConfigFromFiles[BenchmarkConfig](List(new File(configPath))) match {
      case Failure(f) => f.printStackTrace(); sys.exit(1)
      case Success(conf) => conf
    }
    if (!config.isValid) {
      logger.error(s"Invalid configuration:\n${config.toString}")
      sys.exit(1)
    }
    config
  }

  case class QueryConfig(queryName: String = "", inputTopic: String = "", outputTopic: String = "")

  import DefaultValues._
  case class BenchmarkConfig(topicPrefix: String, benchmarkRun: Int, queries: List[String],
                             scaleFactor: Int, sendingInterval: Int = defaultSendingInterval,
                             sendingIntervalTimeUnit: String = defaultSendingIntervalTimeUnit,
                             duration: Long = defaultDuration,
                             durationTimeUnit: String = defaultDurationTimeUnit) {

    val queryConfigs: List[QueryConfig] = for {
      s <- List.range(0, scaleFactor)
      q <- queries
    } yield QueryConfig(q, getSourceName(s), getSinkName(s, q))

    val topics: List[String] = queryConfigs.flatMap(q => List(q.inputTopic, q.outputTopic)).distinct
    val sourceTopics: List[String] = queryConfigs.map(_.inputTopic).distinct
    val sinkTopics: List[String] = queryConfigs.map(_.outputTopic).distinct

    def getSourceName(streamId: Int): String = {
      getTopicName(streamId)
    }

    def getSinkName(streamId: Int, query: String): String = {
      s"${getTopicName(streamId)}-$query"
    }

    def getTopicName(streamId: Int): String = {
      s"$topicPrefix-$streamId-$benchmarkRun"
    }

    def isValid: Boolean = {
      BenchmarkConfigValidator.isValid(this)
    }

    def getDurationTimeUnit(): TimeUnit = TimeUnit.valueOf(durationTimeUnit)

    def getSendingIntervalTimeUnit(): TimeUnit = TimeUnit.valueOf(sendingIntervalTimeUnit)

  }

  object BenchmarkConfigValidator {
    def isValid(benchmarkConfig: BenchmarkConfig): Boolean = {
      isValidSendingInterval(benchmarkConfig.sendingInterval) &&
        isTimeUnitValid(benchmarkConfig.sendingIntervalTimeUnit) &&
      isValidDuration(benchmarkConfig.duration) &&
      isTimeUnitValid(benchmarkConfig.durationTimeUnit)

    }

    def isValidDuration(duration: Long): Boolean = duration > 0
    def isValidSendingInterval(sendingInterval: Long): Boolean = sendingInterval > 0

    def isTimeUnitValid(timeUnit: String): Boolean = {
      val timeUnitEnum = Try(TimeUnit.valueOf(timeUnit))
      timeUnitEnum match {
        case Success(_) => true
        case Failure(_) => false
      }
    }

  }

  object QueryNames {
    val IdentityQuery = "Identity"
    val StatisticsQuery = "Statistics"
  }

  object DefaultValues {
    val defaultDuration = 5
    val defaultDurationTimeUnit = TimeUnit.MINUTES.toString
    val defaultSendingInterval = 10000
    val defaultSendingIntervalTimeUnit = TimeUnit.NANOSECONDS.toString
    val defaultSingleColumn = false
  }
}
