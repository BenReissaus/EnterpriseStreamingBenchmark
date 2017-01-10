package org.hpi.esb.datasender

import java.io.{BufferedReader, FileReader}

class DataReader(var dataInputPath: String) {

  private val br = new BufferedReader(new FileReader(dataInputPath))

  def getLine() = {
    br.readLine()
  }

  def close() = br.close()
}
