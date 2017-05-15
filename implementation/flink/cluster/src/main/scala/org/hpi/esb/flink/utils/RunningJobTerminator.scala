package org.hpi.esb.flink.utils

import org.hpi.esb.commons.util.Logging
import sys.process._

object RunningJobTerminator extends Logging{

  def main(args: Array[String]): Unit = {
    //regex for extracting all strings between " : " and " : ", which are the job ids
    val regex = """(?<= : )(.*)(?= : )""".r
    val jobIdListOfRunningJobs = regex.findAllIn("/opt/flink/bin/flink list -r" !!).toList

    for (jobId <- jobIdListOfRunningJobs) {
      val rc = (s"/opt/flink/bin/flink cancel $jobId" !)
      if (rc != 0) {
        logger.warn(s"Cancelling job with jobID $jobId returned with rc=$rc")
      }
    }

  }

}
