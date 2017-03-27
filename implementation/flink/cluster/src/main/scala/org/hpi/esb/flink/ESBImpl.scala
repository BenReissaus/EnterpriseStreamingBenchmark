package org.hpi.esb.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.hpi.esb.commons.config.Configs
import org.hpi.esb.commons.config.Configs.QueryConfig
import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.flink.kafka.{KafkaConsumer, KafkaProducer}
import org.hpi.esb.flink.query.{IdentityQuery, StatisticsQuery}

object ESBImpl {


  def execute(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val queryConfigs = Configs.benchmarkConfig.queryConfigs

    val groupedQueryConfigs = queryConfigs.groupBy(_.sourceName)

    val pipelines = groupedQueryConfigs.flatMap {
      case (source, configs) => getPipelines(env, source, configs)
    }
    pipelines.foreach(_.execute())
    env.execute("ESB Implementation")
  }

  def getPipelines(env: StreamExecutionEnvironment, source: String, queryConfigs: List[QueryConfig]): List[Pipeline] = {
    val consumer = new KafkaConsumer(env, source)
    queryConfigs.map {
      case QueryConfig(_, sinkName, IdentityQuery) => new Pipeline(consumer, new IdentityQuery(), new KafkaProducer(sinkName))
      case QueryConfig(_, sinkName, StatisticsQuery) => new Pipeline(consumer, new StatisticsQuery(), new KafkaProducer(sinkName))
    }
  }
}
