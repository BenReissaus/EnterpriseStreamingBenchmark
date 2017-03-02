package org.hpi.esb.datavalidator.data

import scala.util.Try

trait Record[T] {
  def deserialize(value: String): Try[T]
}
