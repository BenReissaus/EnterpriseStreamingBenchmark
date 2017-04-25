package org.hpi.esb.datasender

import org.hpi.esb.commons.util.Logging

import scala.io.Source

class DataReader(val source: Source, columns: List[String], columnDelimiter: String, dataColumnStart: Int)
  extends Logging {

  private val dataIterator: Iterator[String] = source.getLines
  private val sendWholeLine: Boolean = columns.size == 1
  private val delimiter = Option(columnDelimiter).getOrElse("")

  def hasRecords: Boolean = dataIterator.hasNext

  def getRecords: Option[List[String]] = {
    if (dataIterator.hasNext) {
      val line = dataIterator.next()
      if (sendWholeLine) {
        Option(List(line))
      }
      else {
        getDataRecords(split(line))
      }
    } else {
      None
    }
  }

  def split(line: String): Option[List[String]] = {
    val splits = line.split(delimiter).toList
    if (columns.length > splits.length) {
      logger.error(s"There are less values available (${splits.length}) than columns defined (${columns.length}) - ignoring record")
      None
    }
    else if (columns.length < splits.length) {
      logger.error(s"There are less topics defined (${columns.length}) than values available (${splits.length}) - ignoring record")
      None
    }
    else {
      Option(splits)
    }
  }

  def getDataRecords(splitsOption: Option[List[String]]): Option[List[String]] = {
    splitsOption.map(_.drop(dataColumnStart))
  }

  def close(): Unit = source.close
}
