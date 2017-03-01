package org.hpi.esb.datasender

/**
  * Created by Benjamin Reissaus on 09/01/17.
  */
object Main extends App {

    val filePath = "/DEBS2012-AllData.txt"
//  val filePath = "/DEBS2012-5Minutes.txt"

  val OneMilliSecond = 1000000
  val PointOneMilliSeconds = 100000
  val period = OneMilliSecond

  val dataProducer = new DataProducer(filePath)
  dataProducer.execute(period)
}
