package org.hpi.esb.util

import org.apache.log4j.Logger
import org.apache.log4j.Level

trait Logging {
  var logger: Logger = Logger.getLogger(this.getClass)
}

object Logging {

  def setToInfo() {
    Logger.getRootLogger.setLevel(Level.INFO)
  }

  def setToDebug() {
    Logger.getRootLogger.setLevel(Level.DEBUG)
  }
}
