package org.hpi.esb.datavalidator.data

import scala.util.Try

object SimpleRecord extends Record[SimpleRecord] {
  override def deserialize(value: String): Try[SimpleRecord] = {
    Try(new SimpleRecord(value.toLong))
  }
}

case class SimpleRecord(value: Long)
