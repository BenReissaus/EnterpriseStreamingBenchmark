package org.hpi.esb.util

import kafka.admin.AdminUtils
import kafka.tools.GetOffsetShell
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.common.errors.TopicExistsException
import org.hpi.esb.commons.config.Configs

import scala.collection.JavaConversions._
import scala.collection.mutable

case class TopicManagementConfig(prefix: String = "", mode: String = "")

object TopicManagement extends Logging {

  val ZookeeperServers = "192.168.30.208:2181,192.168.30.207:2181,192.168.30.141:2181"
  val ZookeeperTopicPath = "/brokers/topics"
  val KafkaTopicPartitions = 1
  val KafkaTopicReplicationFactor = 1
  val SessionTimeout = 6000
  val ConnectionTimeout = 6000
  lazy val zkClient: ZkClient = ZkUtils.createZkClient(ZookeeperServers, SessionTimeout, ConnectionTimeout)
  lazy val zkUtils = new ZkUtils(zkClient, new ZkConnection(ZookeeperServers), false)

  def main(args: Array[String]): Unit = {
    execute(getConfig(args))
  }

  def getConfig(args: Array[String]): TopicManagementConfig = {
    val parser = new scopt.OptionParser[TopicManagementConfig]("TopicManagement") {
      head("Topic Management for 'Enterprise Streaming Benchmark'")

      cmd("list")
        .action((_, c) => c.copy(mode = "list"))
        .children(
          opt[String]('p', "prefix")
            .required()
            .action((x, c) => c.copy(prefix = x))
        )
        .text("All topics that match 'prefix' get listed.")
      cmd("delete")
        .action((_, c) => c.copy(mode = "delete"))
        .children(
          opt[String]('p', "prefix")
            .required()
            .action((x, c) => c.copy(prefix = x))
        )
        .text("All topics that match 'prefix' get deleted.")
      cmd("create")
        .action((_, c) => c.copy(mode = "create"))
        .text("All necessary benchmark topics get created.")

      help("help")
    }

    parser.parse(args, TopicManagementConfig()) match {
      case Some(c) => c
      case _ => sys.exit(1)
    }
  }

  def execute(config: TopicManagementConfig): Unit = {
    config.mode match {
      case "create" => createTopics()
      case "list" => listTopics(config.prefix)
      case "delete" => deleteTopics(config.prefix)
      case _ => println("Please use --help argument for usage.")
    }
  }

  def createTopics(): Unit = {
    val topics = Configs.benchmarkConfig.topics
    topics.foreach(topic =>
      try {
        AdminUtils.createTopic(zkUtils,
          topic,
          KafkaTopicPartitions,
          KafkaTopicReplicationFactor)
      }
      catch {
        case e: TopicExistsException => logger.info(s"Topic $topic already exists.")
      }
    )

    zkUtils.close()
  }

  def listTopics(prefix: String): Unit = {
    val topics = getMatchingTopics(getAllTopics, prefix)
    logger.info(s"The following topics match the regex: $topics")
    zkClient.close()
  }

  def deleteTopics(prefix: String): Unit = {
    val topics = getMatchingTopics(getAllTopics, prefix)
    logger.info(s"The following topics are getting deleted from Zookeeper: $topics")
    topics.map(topic => {
      zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
    })
    zkClient.close()
  }

  private def getAllTopics: mutable.Buffer[String] = {
    zkClient.getChildren(TopicManagement.ZookeeperTopicPath)
  }

  def getMatchingTopics(topics: mutable.Buffer[String], prefix: String): mutable.Buffer[String] = {
    topics.filter(t => t.matches(s"$prefix.*"))
  }
}
