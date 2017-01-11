package org.hpi.esb.flink

import org.apache.flink.api.java.utils.ParameterTool

object Main {

  def main(args: Array[String]) = {

    val parameters = ParameterTool.fromArgs(args)
    val consumerTopic = parameters.get("consumerTopic")
    val producerTopic = parameters.get("producerTopic")

    if (consumerTopic == null || producerTopic == null) {
      println("Error: Not all required arguments were passed.")
      println("Usage: flink run *.jar --consumerTopic [consumer_topic_name] --producerTopic [producer_topic_name]")
      System.exit(1)
    }

    IdentityQuery.execute(consumerTopic, producerTopic)
  }
}
