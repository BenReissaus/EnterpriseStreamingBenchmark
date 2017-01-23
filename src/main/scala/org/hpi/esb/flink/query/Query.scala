package org.hpi.esb.flink.query

import org.apache.flink.streaming.api.scala.DataStream

trait Query[T, U] {
  def execute(stream: DataStream[T]): DataStream[U]
}
