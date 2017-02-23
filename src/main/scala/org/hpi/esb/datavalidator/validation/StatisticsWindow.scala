package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.data.Statistics

class StatisticsWindow(windowSize: Long) extends Window(windowSize) {

  var stats = new Statistics()

  override def update(): Unit = {
    super.update()
    stats = new Statistics()
  }

  def addValue(value: Long): Unit = {
    stats = stats.getUpdatedWithValue(value)
  }
}
