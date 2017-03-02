package org.hpi.esb.datavalidator.data

import scala.util.Try

object Statistics extends Record[Statistics] {
  def deserialize(stats: String): Try[Statistics] = {
    val values = stats.split(",")

    if (values.length != 5) {
      throw new IllegalArgumentException(s"5 comma-separated values were expected but not found.")
    }
    Try(this(values(0).toLong, values(1).toLong, values(2).toLong, values(3).toLong, values(4).toDouble))
  }
}

case class Statistics(var min: Long = Long.MaxValue, var max: Long = Long.MinValue,
                      var sum: Long = 0, var count: Long = 0,
                      var avg: Double = 0) {

  def getUpdatedWithValue(value: Long): Statistics = {

    val newCount = count + 1
    val newSum = sum + value
    val newMin = if (value < min) value else min
    val newMax = if (value > max) value else max
    val newAvg = newSum.toDouble / newCount

    new Statistics(newMin, newMax, newSum, newCount, newAvg)
  }

  override def toString: String = {
    s"$min,$max,$sum,$count,$avg"
  }

  def prettyPrint: String = {
    s"Min: $min, Max: $max, Sum: $sum, Count: $count, Avg: $avg"
  }
}

