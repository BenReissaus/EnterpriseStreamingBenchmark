package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.data.Statistics
import org.scalatest.FunSuite

class StatisticsWindowTest extends FunSuite {

  val statisticsWindow = new StatisticsWindow(windowSize = 1000)
  val initializedStats = new Statistics()

  test("testUpdate") {

    assert(statisticsWindow.stats == initializedStats)

    statisticsWindow.addValue(100)
    assert(statisticsWindow.stats != initializedStats)

    statisticsWindow.update()
    assert(statisticsWindow.stats == initializedStats)
  }

}
