package org.hpi.esb.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.hpi.esb.flink.kafka.{KafkaProducer, KafkaConsumer}
import org.hpi.esb.flink.query.{IdentityQuery, StatisticsQuery}

object ESBImpl {

  def execute(consumerTopic: String, identityTopic: String, statisticsTopic: String): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConsumer = new KafkaConsumer(env, consumerTopic)

    val pipelines = Seq(
      new Pipeline(kafkaConsumer, new IdentityQuery(), new KafkaProducer(identityTopic)),
      new Pipeline(kafkaConsumer, new StatisticsQuery(), new KafkaProducer(statisticsTopic)))

    pipelines.foreach(_.execute())

    env.execute("ESB Implementation")
  }
}
