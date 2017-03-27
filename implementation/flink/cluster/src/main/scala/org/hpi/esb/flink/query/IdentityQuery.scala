package org.hpi.esb.flink.query

import org.apache.flink.streaming.api.scala._


class IdentityQuery extends Query[String, String] {
  override def execute(stream: DataStream[String]): DataStream[String] = stream
}
