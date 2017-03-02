package org.hpi.esb.flink

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class CustomAssigner extends AssignerWithPeriodicWatermarks[String] {

  val activityThreshold = 10000
  val latenessThreshold = 100

  var maxObservedEventTimestamp: Long = 0L
  var lastElementObserved: Long = Long.MaxValue

  override def getCurrentWatermark: Watermark = {

    val currentSystemTime = System.currentTimeMillis()

    // if last element was observed more than 10 seconds ago, update watermark with current system time to finish windows
    val watermark = if (lastElementObserved < (currentSystemTime - activityThreshold))
      new Watermark(currentSystemTime)
    else
      new Watermark(maxObservedEventTimestamp - latenessThreshold)

    watermark
  }

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    lastElementObserved = System.currentTimeMillis()
    maxObservedEventTimestamp = Math.max(maxObservedEventTimestamp, previousElementTimestamp)
    previousElementTimestamp
  }
}
