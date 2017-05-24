package org.hpi.esb.datasender

import org.hpi.esb.commons.util.Logging

import scala.io.Source

class DataReader(val source: Source, columns: List[String], columnDelimiter: String, dataColumnStart: Int, readInRam: Boolean)
  extends Logging {

  private var dataIterator: Iterator[String] = if(readInRam) recordList.toIterator else source.getLines
  private val sendWholeLine: Boolean = columns.size == 1
  private val delimiter = Option(columnDelimiter).getOrElse("")
  private lazy val recordList = source.getLines.toList

  def hasRecords: Boolean = dataIterator.hasNext

  def readRecords: Option[List[String]] = {
    if (dataIterator.hasNext) {
      retrieveRecords()
    } else {
      resetIterator()
      retrieveRecords()
    }
  }

  def retrieveRecords(): Option[List[String]] = {
    val line = dataIterator.next()
    if (sendWholeLine) {
      Option(List(line))
    }
    else {
      val multipleRecords = split(line)
      multipleRecords.map(_.drop(dataColumnStart))
    }
  }

  def resetIterator(): Unit = {
    if(readInRam) {
      dataIterator = recordList.toIterator
    } else {
      dataIterator = source.reset().getLines()
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

  def close(): Unit = source.close
}
