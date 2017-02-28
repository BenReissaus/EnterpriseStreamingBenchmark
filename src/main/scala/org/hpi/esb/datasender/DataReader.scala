package org.hpi.esb.datasender

class DataReader(var dataInputPath: String) {

  private val bufferedSource: io.BufferedSource = io.Source.fromFile(dataInputPath)
  private val dataIterator: Iterator[String] = bufferedSource.asInstanceOf[io.Source].getLines

  def getLine: Option[String] = {
      if (dataIterator.hasNext) {
        Option(dataIterator.next)
      } else {
        None: Option[String]
      }
  }

  def close(): Unit = bufferedSource.close
}
