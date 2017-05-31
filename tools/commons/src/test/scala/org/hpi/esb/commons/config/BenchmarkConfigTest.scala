package org.hpi.esb.commons.config

import org.hpi.esb.commons.config.Configs.{BenchmarkConfig, QueryConfig}
import org.scalatest.FunSpec

class BenchmarkConfigTest extends FunSpec {


  val topicPrefix = "ESB"
  val benchmarkRun = 1
  val identityQuery = "Identity"
  val statisticsQuery = "Statistics"
  val queries = List(identityQuery, statisticsQuery)
  val scaleFactor = 2
  val benchmarkConfig = BenchmarkConfig(topicPrefix, benchmarkRun, queries, scaleFactor)

  describe("getTopicName") {
    it("should return the correct topic name") {
      val streamId = 0
      val topicName = benchmarkConfig.getTopicName(streamId)
      val expectedTopicName = s"$topicPrefix-$streamId-$benchmarkRun"
      assert(topicName == expectedTopicName)
    }
  }

  describe("getSourceName") {
    it("should return the correct source topic name") {
      val streamId = 0
      val sourceTopicName = benchmarkConfig.getSourceName(streamId)
      val expectedSourceTopicName = s"$topicPrefix-$streamId-$benchmarkRun"
      assert(sourceTopicName == expectedSourceTopicName)
    }
  }

  describe("getSinkName") {
    it("should return the correct sink topic name") {
      val streamId = 0
      val query = "Identity"
      val sinkTopicName = benchmarkConfig.getSinkName(streamId, query)
      val expectedSinkTopicName = s"$topicPrefix-$streamId-$benchmarkRun-$query"
      assert(sinkTopicName == expectedSinkTopicName)
    }
  }

  describe("queryConfigs") {
    it("should return the correct list of query configs for scale factor 2") {
      var scaleFactor = 0
      val inputTopic0 = s"$topicPrefix-$scaleFactor-$benchmarkRun"
      val outputTopic0Identity = s"$topicPrefix-$scaleFactor-$benchmarkRun-$identityQuery"
      val outputTopic0Statistics = s"$topicPrefix-$scaleFactor-$benchmarkRun-$statisticsQuery"

      val queryConfig0Identity = QueryConfig(identityQuery, inputTopic0, outputTopic0Identity)
      val queryConfig0Statistic = QueryConfig(statisticsQuery, inputTopic0, outputTopic0Statistics)

      scaleFactor = 1
      val inputTopic1 = s"$topicPrefix-$scaleFactor-$benchmarkRun"
      val outputTopic1Identity = s"$topicPrefix-$scaleFactor-$benchmarkRun-$identityQuery"
      val outputTopic1Statistics = s"$topicPrefix-$scaleFactor-$benchmarkRun-$statisticsQuery"

      val queryConfig1Identity = QueryConfig(identityQuery, inputTopic1, outputTopic1Identity)
      val queryConfig1Statistic = QueryConfig(statisticsQuery, inputTopic1, outputTopic1Statistics)

      val expectedQueryConfigs = List(queryConfig0Identity, queryConfig0Statistic, queryConfig1Identity, queryConfig1Statistic)

      assert(benchmarkConfig.queryConfigs.toSet == expectedQueryConfigs.toSet)
    }
  }

}
