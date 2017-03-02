package org.hpi.esb.flink.query

import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.hpi.esb.flink.utils.Statistics

class StatisticsQuery extends Query[String, String] {

  override def execute(stream: DataStream[String]): DataStream[String] = {

   stream
     .map(_.toLong)
     .timeWindowAll(Time.milliseconds(1000))
     .fold(new Statistics(), new StatisticsFoldFunction())
     .map(v => v.toString())
  }
}

class StatisticsFoldFunction extends FoldFunction[Long, Statistics] {
  override def fold(acc: Statistics, value: Long): Statistics = Statistics.fold(acc, value)
}

