package org.hpi.esb.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.hpi.esb.commons.config.Configs
import org.hpi.esb.commons.config.Configs.{QueryConfig, QueryNames}
import org.hpi.esb.flink.kafka.{KafkaConsumer, KafkaProducer}
import org.hpi.esb.flink.query.{IdentityQuery, StatisticsQuery}

object ESBImpl {


  def execute(): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val benchmarkConfig = Configs.benchmarkConfig

    val pipelines = benchmarkConfig.streamConfigs.flatMap(streamConfig => {
      val consumer = new KafkaConsumer(env, streamConfig.sourceName)

      streamConfig.queryConfigs.map {
       case QueryConfig(QueryNames.IdentityQuery, sinkName) => new Pipeline(consumer, new IdentityQuery(), new KafkaProducer(sinkName))
       case QueryConfig(QueryNames.StatisticsQuery, sinkName) => new Pipeline(consumer, new StatisticsQuery(), new KafkaProducer(sinkName))
     }
    })

    pipelines.foreach(_.execute())
    env.execute("ESB Implementation")
  }
}
