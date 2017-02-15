package org.hpi.esb.datavalidator.validation

class Window(val windowSize: Long, firstElementTimestamp: Long) {

  var windowEnd: Long = getInitialWindowEnd(firstElementTimestamp)

  def contains(t: Long): Boolean = {
    t < windowEnd
  }

  def updateWindowEnd(): Unit = {
    windowEnd += windowSize
  }

  private def getInitialWindowEnd(firstElementTimestamp: Long): Long = {
    val windowStart = firstElementTimestamp - (firstElementTimestamp % windowSize)
    val windowEnd = windowStart + windowSize
    windowEnd
  }
}
