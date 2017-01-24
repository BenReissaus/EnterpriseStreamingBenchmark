package org.hpi.esb.datasender

class DataReader(var dataInputPath: String) {

  private val bufferedSource: io.BufferedSource = io.Source.fromFile(dataInputPath)
  private val dataIterator: Iterator[String] = bufferedSource.asInstanceOf[io.Source].getLines

  def getLine: String = {
      if (dataIterator.hasNext) {
        dataIterator.next
      } else {
        null
      }
  }

  def close = bufferedSource.close
}
