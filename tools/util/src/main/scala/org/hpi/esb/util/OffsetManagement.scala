package org.hpi.esb.util

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadata
import org.hpi.esb.commons.util.Logging

object OffsetManagement extends Logging {

  def getNumberOfMessages(topic: String, partition: Int): Long = {

    val clientId = "GetOffset"

    val topicsMetadata = getMetaData(topic, clientId)
    getLatestOffset(topicsMetadata, topic, partition, clientId)
  }

  private def getMetaData(topic: String, clientId: String) = {
    val brokerList = "192.168.30.208:9092, 192.168.30.207:9092, 192.168.30.141:9092"
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)


    val maxWaitMs = 1000
    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
    if(topicsMetadata.size != 1 || !topicsMetadata.head.topic.equals(topic)) {
      logger.error(s"Error: no valid topic metadata for topic: $topic, probably the topic does not exist, run kafka-list-topic.sh to verify")
      sys.exit(1)
    }

    topicsMetadata
  }

  private def getLatestOffset(topicsMetadata: Seq[TopicMetadata], topic: String, partition: Int, clientId: String) = {

    val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partition)
    val time = -1
    val nOffsets = 1

    partitionMetadataOpt match {
      case Some(metadata) =>
        metadata.leader match {
          case Some(leader) =>
            val timeout = 10000
            val bufferSize = 100000
            val consumer = new SimpleConsumer(leader.host, leader.port, timeout, bufferSize, clientId)
            val topicAndPartition = TopicAndPartition(topic, partition)
            val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
            val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets

            offsets.last
          case None => logger.error(s"Error: partition $partition does not have a leader. Skip getting offsets"); sys.exit(1)
        }
      case None => logger.error(s"Error: partition $partition does not exist"); sys.exit(1)
    }
  }

}
