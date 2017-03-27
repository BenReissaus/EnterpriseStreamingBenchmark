package org.hpi.esb.commons.config

import java.io.File

import pureconfig.loadConfigFromFiles

import scala.util.{Failure, Success}

object Configs {

  private val relativeConfigPath = "/tools/commons/commons.conf"
  private val configPath: String = System.getProperty("user.dir") + relativeConfigPath
  val benchmarkConfig: BenchmarkConfig = getConfig(configPath)

  def getConfig(configPath: String): BenchmarkConfig = {

    val benchmarkArguments: BenchmarkArguments = loadConfigFromFiles[BenchmarkArguments](List(new File(configPath))) match {
      case Failure(f) => f.printStackTrace(); sys.exit(1)
      case Success(conf) => conf
    }
    benchmarkArguments.getBenchmarkConfig
  }

  case class BenchmarkConfig(queryConfigs: List[QueryConfig]) {
    def getAllTopics: List[String] = {
      queryConfigs.flatMap(q => List(q.sourceName, q.sinkName)).distinct
    }

    def getSourceTopics: List[String] = {
      queryConfigs.map(_.sourceName).distinct
    }
  }

  case class QueryConfig(sourceName: String, sinkName: String, queryName: String)

  case class BenchmarkArguments(topicPrefix: String, topicPostfix: String, streams: List[String], queries: List[String]) {

    def getBenchmarkConfig: BenchmarkConfig = {

      val queryConfigs = for {
        s <- streams
        q <- queries
      } yield QueryConfig(getSourceName(s), getSinkName(s, q), q)

      BenchmarkConfig(queryConfigs)
    }

    def getSourceName(stream: String): String = {
      s"${topicPrefix}_${stream}_$topicPostfix"
    }

    def getSinkName(stream: String, query: String): String = {
      s"${topicPrefix}_${stream}_${query}_$topicPostfix"
    }
  }

  object QueryNames {
    val IdentityQuery = "Identity"
    val StatisticsQuery = "Statistics"
  }
}
