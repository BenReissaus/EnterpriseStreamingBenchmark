package org.hpi.esb.datavalidator.data

trait Record[T] {
  def create(value: String): T
}
