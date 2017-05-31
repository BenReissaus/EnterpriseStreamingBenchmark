package org.hpi.esb.datasender.output

import java.io.File

abstract class ResultMerger {
  def execute(): Unit
  val outputFileName: String

  def filterFilesByPrefix(files: List[File], prefix: String): List[File] = {
    files.filter(_.getName.startsWith(prefix))
  }
}
