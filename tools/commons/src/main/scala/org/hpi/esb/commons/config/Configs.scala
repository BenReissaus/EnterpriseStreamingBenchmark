package org.hpi.esb.commons.config

import java.io.File

import pureconfig.loadConfigFromFiles

import scala.util.{Failure, Success}

object Configs {

  private val relativeConfigPath = "/tools/commons/commons.conf"
  private val configPath: String = System.getProperty("user.dir") + relativeConfigPath
  val benchmarkConfig: BenchmarkConfig = getConfig(configPath)

  def getConfig(configPath: String): BenchmarkConfig = {
    loadConfigFromFiles[BenchmarkConfig](List(new File(configPath))) match {
      case Failure(f) => f.printStackTrace(); sys.exit(1)
      case Success(conf) => conf
    }
  }

  case class QueryConfig(queryName: String = "", inputTopic: String = "", outputTopic: String = "")

  case class BenchmarkConfig(topicPrefix: String, topicPostfix: String, queries: List[String], scaleFactor: Int) {

    val queryConfigs: List[QueryConfig]= for {
      s <- List.range(0, scaleFactor)
      q <- queries
    } yield QueryConfig(q, getSourceName(s), getSinkName(s, q))

    val topics: List[String] = queryConfigs.flatMap(q => List(q.inputTopic, q.outputTopic)).distinct
    val sourceTopics: List[String] = queryConfigs.map(_.inputTopic).distinct

    def getSourceName(stream: Int): String = {
      s"${topicPrefix}_stream${stream}_$topicPostfix"
    }

    def getSinkName(stream: Int, query: String): String = {
      s"${topicPrefix}_stream${stream}_${query}_$topicPostfix"
    }
  }

  object QueryNames {
    val IdentityQuery = "Identity"
    val StatisticsQuery = "Statistics"
  }
}
