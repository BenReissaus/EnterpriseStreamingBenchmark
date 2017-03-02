package org.hpi.esb.flink.utils

object Statistics {
  def fold(acc: Statistics, value: Long): Statistics = {
    acc.sum += value
    acc.count += 1

    if (value < acc.min)
      acc.min = value

    if (value > acc.max)
      acc.max = value

    if (acc.count > 0)
      acc.avg = acc.sum.toDouble / acc.count

    acc
  }
}

class Statistics(var min: Long = Long.MaxValue, var max: Long = Long.MinValue, var sum: Long = 0, var count: Long = 0, var avg: Double = 0) {

  override def toString = {
    s"$min,$max,$sum,$count,$avg"
  }
}

