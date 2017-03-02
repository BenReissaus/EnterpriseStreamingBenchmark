package org.hpi.esb.flink

import org.apache.flink.api.java.utils.ParameterTool

object Main {

  def main(args: Array[String]) = {
    (ESBImpl.execute _).tupled(handleParameters(args))
  }

  def handleParameters(args: Array[String]) = {

    val parameters = ParameterTool.fromArgs(args)
    val consumerTopic = parameters.get("consumerTopic")
    val identityTopic = parameters.get("identityTopic")
    val statisticsTopic = parameters.get("statisticsTopic")

    if (consumerTopic == null || identityTopic == null || statisticsTopic == null) {
      println("Error: Not all required arguments were passed.")
      println("Usage: flink run *.jar --consumerTopic [consumer_topic_name] --identityTopic [identity_query_topic] --statisticsTopic [statistics_query_topic]")
      System.exit(1)
    }

    (consumerTopic, identityTopic, statisticsTopic)
  }
}
