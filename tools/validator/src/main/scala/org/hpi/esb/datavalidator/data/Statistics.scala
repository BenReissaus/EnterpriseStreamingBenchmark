package org.hpi.esb.datavalidator.data

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Statistics extends Deserializer[Statistics] {
  def deserialize(stats: String, timestamp: Long): Try[Statistics] = {
    val values = stats.split(",")

    Try {
      if (values.length != 5) {
        throw new IllegalArgumentException(s"The string '$stats' could not be deserialzed into statistics.")
      }
      this (values(0).toLong, values(1).toLong, values(2).toLong, values(3).toLong, values(4).toDouble)(timestamp)
    }
  }

  override def deserializeList(list: ListBuffer[ConsumerRecord[String, String]]): ListBuffer[Try[Statistics]] = {
    list.map(r => Statistics.deserialize(r.value(), r.timestamp()))
  }
}

case class Statistics(var min: Long = Long.MaxValue, var max: Long = Long.MinValue,
                      var sum: Long = 0, var count: Long = 0,
                      var avg: Double = 0)(override val timestamp: Long = 0) extends Record(timestamp){

  def getUpdatedWithValue(timestamp: Long, value: Long): Statistics = {

    val newCount = count + 1
    val newSum = sum + value
    val newMin = if (value < min) value else min
    val newMax = if (value > max) value else max
    val newAvg = newSum.toDouble / newCount

    new Statistics(newMin, newMax, newSum, newCount, newAvg)(timestamp)
  }

  override def toString: String = {
    s"$min,$max,$sum,$count,$avg,$timestamp"
  }

  def prettyPrint: String = {
    s"Min: $min, Max: $max, Sum: $sum, Count: $count, Avg: $avg, LatestTimestamp: $timestamp"
  }
}

