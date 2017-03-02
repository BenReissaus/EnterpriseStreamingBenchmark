package org.hpi.esb.datavalidator.validation

class Window(val windowSize: Long) {

  var windowEnd: Long = 0

  def containsTimestamp(t: Long): Boolean = {
    t < windowEnd
  }

  def update(): Unit = {
    windowEnd += windowSize
  }

  def setInitialWindowEnd(firstElementTimestamp: Long): Unit = {
    val windowStart = firstElementTimestamp - (firstElementTimestamp % windowSize)
    windowEnd = windowStart + windowSize
  }
}
