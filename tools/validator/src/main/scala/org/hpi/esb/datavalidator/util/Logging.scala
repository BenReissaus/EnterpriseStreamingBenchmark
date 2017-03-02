package org.hpi.esb.datavalidator.util

import org.apache.log4j.{Level, Logger}

trait Logging {
  val logger: Logger = Logger.getLogger(this.getClass)
}

object Logging {

  def setToInfo() {
    Logger.getRootLogger.setLevel(Level.INFO)
  }

  def setToDebug() {
    Logger.getRootLogger.setLevel(Level.DEBUG)
  }
}
