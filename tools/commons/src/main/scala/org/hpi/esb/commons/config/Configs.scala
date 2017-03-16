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

  case class BenchmarkConfig(streamConfigs: List[StreamConfig]) {
    def getAllTopics: List[String] = {
      streamConfigs.flatMap(s => List(s.sourceName) ++ s.queryConfigs.map(queryConfig => queryConfig.sinkName))
    }

    def getSourceTopics: List[String] = {
      streamConfigs.map(_.sourceName)
    }
  }
  case class StreamConfig(sourceName: String, queryConfigs: List[QueryConfig])
  case class QueryConfig(queryName: String, sinkName: String)


  case class BenchmarkArguments(topicPrefix: String, topicPostfix: String, streams: List[String], queries: List[String]) {

    def getBenchmarkConfig: BenchmarkConfig = {

      val sensorConfigs = streams.map(sensor => {
        val queryConfigs = queries.map(query => {
          QueryConfig(query, getSinkName(sensor, query))
        })
        StreamConfig(getSourceName(sensor), queryConfigs)
      })
      BenchmarkConfig(sensorConfigs)
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
