package org.hpi.esb.flink

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class CustomAssigner extends AssignerWithPeriodicWatermarks[String] {

  val activityThreshold = 10000
  var maxObservedEventTimestamp: Long = 0L
  var lastTimeElementObserved: Long = Long.MaxValue

  override def getCurrentWatermark: Watermark = {

    val currentSystemTime = System.currentTimeMillis()

    val watermark = if (reachedEnd(currentSystemTime)) {
      new Watermark(currentSystemTime)
    } else {
      // have to subtract 1 since more elements with exact timestamp could still arrive
      new Watermark(maxObservedEventTimestamp - 1)
    }

    watermark
  }

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    lastTimeElementObserved = System.currentTimeMillis()
    maxObservedEventTimestamp = previousElementTimestamp
    previousElementTimestamp
  }

  def reachedEnd(currentSystemTime: Long): Boolean = {
    // the stream end is assumed to be reached if last element was observed more than 10 seconds ago
    (currentSystemTime - activityThreshold) > lastTimeElementObserved
  }
}
