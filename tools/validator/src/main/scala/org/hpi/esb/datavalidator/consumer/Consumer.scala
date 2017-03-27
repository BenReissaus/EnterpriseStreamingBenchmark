package org.hpi.esb.datavalidator.consumer

import java.util.concurrent.{Executors, TimeUnit}

import org.hpi.esb.datavalidator.config.KafkaConsumerConfig
import org.hpi.esb.datavalidator.util.Logging


class Consumer(topics: List[String], config: KafkaConsumerConfig) extends Logging {

  // TODO: this number could get pretty big. should probably be more like #cores - 1
  private val numberOfThreads = topics.length
  private val executor = Executors.newFixedThreadPool(numberOfThreads)

  def consume(): Records = {

    val records = new Records(topics)
    records.recordsByTopic.foreach {
      case (topic, results) => executor.submit(new ConsumerLoop(topic, config, results))
    }

    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.HOURS)

    records
  }
}
