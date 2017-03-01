package org.hpi.esb.util

import org.apache.log4j.Logger

trait Logging {
  val logger = Logger.getLogger(this.getClass)
}
