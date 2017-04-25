package org.hpi.esb.commons.output

object ValueFormatter {
  def round(value: Double, precision: Int): Double = {
    val base = 10
    val v = math.pow(base, precision)
    math.round(value * v) / v
  }

  def roundPrecise(value: BigDecimal, precision: Int): Double = {
      value.setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
