package org.hpi.esb.datavalidator.data

object SimpleRecord extends Record[SimpleRecord] {
  override def create(value: String): SimpleRecord = {
    new SimpleRecord(value.toLong)
  }
}

case class SimpleRecord(value: Long)
