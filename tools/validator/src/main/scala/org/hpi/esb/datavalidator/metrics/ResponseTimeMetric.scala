package org.hpi.esb.datavalidator.metrics

import com.codahale.metrics.{Histogram, Snapshot, UniformReservoir}

object ResponseTimeMetric {
  val referenceValue: Long = 2000 // in milliseconds
  val percentile: Double = 0.90
}

class ResponseTimeMetric extends ConstrainedMetric {

  override def fulFillsConstraint: Boolean = {
     getPercentileValue < ResponseTimeMetric.referenceValue
  }

  val histogram = new Histogram(new UniformReservoir())
  lazy val snapshot: Snapshot = histogram.getSnapshot


  override def getSuccessMessage: String = {
    s"""The response time constraint is fulfilled. The ${ResponseTimeMetric.percentile * 100} percentile is equal to $getPercentileValue ms and therefore below the threshold of ${ResponseTimeMetric.referenceValue} ms.
       |General response time statistics: $getGeneralInfo
     """
      .stripMargin
  }

  override def getErrorMessage: String = {
    s"""The response time constraint is NOT fulfilled. The ${ResponseTimeMetric.percentile * 100} percentile is equal to $getPercentileValue ms and therefore above the threshold of ${ResponseTimeMetric.referenceValue} ms.
       |General response time statistics: $getGeneralInfo
     """
      .stripMargin
  }

  def getGeneralInfo: String = {
    s"Min: ${snapshot.getMin}; Max: ${snapshot.getMax}; Mean: ${snapshot.getMean}"
  }

  def getPercentileValue: Double = {
    snapshot.getValue(ResponseTimeMetric.percentile)
  }

  def updateValue(value: Long): Unit = {
    histogram.update(value)
  }

  def getAllValues: Array[Long] = {
    snapshot.getValues
  }
}
