package org.hpi.esb.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.flink.kafka.{KafkaConsumer, KafkaProducer}
import org.hpi.esb.flink.query.{IdentityQuery, StatisticsQuery}

class QueryJob(queryName: String, inputTopic: String, outputTopic: String) {

  def execute(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val consumer = new KafkaConsumer(env, inputTopic)
    val producer = new KafkaProducer(outputTopic)

    val query = queryName match {
      case IdentityQuery => new IdentityQuery()
      case StatisticsQuery => new StatisticsQuery()
    }

    new Pipeline(consumer, query, producer).execute()

    env.execute("ESB Implementation")
  }
}
