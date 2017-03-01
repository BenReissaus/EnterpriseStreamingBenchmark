package org.hpi.esb.datasender

import java.io.{BufferedReader, InputStreamReader}

class DataReader(var filePath: String) {

  private val br = new BufferedReader(new InputStreamReader(this.getClass.getResourceAsStream(filePath)))

  def getLine() = {
    br.readLine()
  }

  def close() = br.close()
}
