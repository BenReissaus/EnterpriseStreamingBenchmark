package org.hpi.esb.util

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConversions._
import scala.collection.mutable

case class TopicManagementConfig(prefix: String = "", mode: String = "")

object TopicManagement {

  val ZookeeperServers = "192.168.30.208:2181,192.168.30.207:2181,192.168.30.141:2181"
  val zookeeperTopicPath = "/brokers/topics"
  val SessionTimeout = 6000
  val ConnectionTimeout = 6000
  val zkClient: ZkClient = ZkUtils.createZkClient(ZookeeperServers, SessionTimeout, ConnectionTimeout)

  def main(args: Array[String]): Unit = {
    val topicManagement = new TopicManagement(getConfig(args), zkClient)
    topicManagement.execute()
  }

  def getConfig(args: Array[String]): TopicManagementConfig = {
    val parser = new scopt.OptionParser[TopicManagementConfig]("TopicManagement") {
      opt[String]('p', "prefix")
        .required()
        .action((x, c) => c.copy(prefix = x))
        .text("All topics that match 'regex' will be deleted or listed.")

      cmd("list").action((_, c) => c.copy(mode = "list"))
      cmd("delete").action((_, c) => c.copy(mode = "delete"))
    }

    parser.parse(args, TopicManagementConfig()) match {
      case Some(c) => c
      case None => sys.exit(1)
    }
  }
}

class TopicManagement(config: TopicManagementConfig, zkClient: ZkClient) extends Logging {

  def execute(): Unit = {
    val matchingTopics = getMatchingTopics(getAllTopics, config.prefix)
    config.mode match {
      case "list" => listTopics(matchingTopics)
      case "delete" => deleteTopics(matchingTopics)
    }
  }

  def listTopics(topics: mutable.Buffer[String]): Unit = {
    logger.info(s"The following topics match the regex: $topics")
  }

  def deleteTopics(topics: mutable.Buffer[String]): Unit = {
    logger.info(s"The following topics are getting deleted from Zookeeper: $topics")
    topics.map(topic => {
      zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
    })
  }

  private def getAllTopics: mutable.Buffer[String] = {
    zkClient.getChildren(TopicManagement.zookeeperTopicPath)
  }

  def getMatchingTopics(topics: mutable.Buffer[String], prefix: String): mutable.Buffer[String] = {
    topics.filter(t => t.matches(s"$prefix.*"))
  }
}
