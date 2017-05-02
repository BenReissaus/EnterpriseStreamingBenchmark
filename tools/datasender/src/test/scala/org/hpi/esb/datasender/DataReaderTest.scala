package org.hpi.esb.datasender

import org.apache.log4j.Logger
import org.mockito.Mockito.{times, verify}
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.io.Source

class DataReaderTest extends FunSpec with Matchers with PrivateMethodTester
  with BeforeAndAfter with MockitoSugar {


  val columns = List("timestamp", "id", "stream0", "stream1", "stream2")
  val tooFewValues = "ts0 id0 dat0 dat0"
  val tooManyValues = "ts0 id0 dat0 dat0 dat1 dat2"
  val dataReader = new DataReader(mock[Source], columns, columnDelimiter = " ", dataColumnStart = 2, readInRam = false)

  describe("split") {

    it("should return an empty Option when receiving an empty string") {
      assert(dataReader.split("").isEmpty)
    }

    it("should return an empty Option when receiving too few data columns") {
      assert(dataReader.split(tooFewValues).isEmpty)
    }

    it("should log an error when receiving too few data columns") {
      val mockedLogger: Logger = mock[Logger]
      dataReader.logger = mockedLogger
      dataReader.split(tooFewValues)
      verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
    }

    it("should return an empty Option when receiving too many data columns") {
      assert(dataReader.split(tooManyValues).isEmpty)
    }

    it("should log an error when receiving too many data columns") {
      val mockedLogger: Logger = mock[Logger]
      dataReader.logger = mockedLogger
      dataReader.split(tooManyValues)
      verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
    }

    it("should return an Option with a list of values when called correctly") {
      val values = dataReader.split("ts0 id0 dat0 dat1 dat2").get
      assert(values(0) == "ts0")
      assert(values(1) == "id0")
      assert(values(2) == "dat0")
      assert(values(3) == "dat1")
      assert(values(4) == "dat2")
    }

    it("should treat the whole line as one element when 'columnDelimiter' is null and therefore return None") {
      val dataReader = new DataReader(mock[Source], columns, columnDelimiter = null, dataColumnStart = 2, readInRam = false)
      val line = "ts0 id0 dat0 dat1 dat2"
      assert(dataReader.split(line).isEmpty)
    }

    it("should allow receiving a regex as columnDelimiter") {
      val delimiter = "\\s+"
      val dataReader = new DataReader(mock[Source], columns, delimiter, dataColumnStart = 2, readInRam = false)
      val line = List("ts0", "id0", "dat0", "dat1", "dat2").mkString("\t")
      println(line)
      val values = dataReader.split(line).get
      assert(values(0) == "ts0")
      assert(values(1) == "id0")
      assert(values(2) == "dat0")
      assert(values(3) == "dat1")
      assert(values(4) == "dat2")
    }
  }

  describe("getDataMessages") {

    it("should drop the first 2 metadata columns") {
      val splitsOption = dataReader.getDataRecords(Option(List("ts0", "id0", "dat0", "dat1", "dat2")))
      assert(splitsOption.isDefined)
      val splits = splitsOption.get
      assert(splits.length == 3)
      assert(splits(0) == "dat0")
      assert(splits(1) == "dat1")
      assert(splits(2) == "dat2")
    }

    it("should return an empty Option when receiving None") {
      val splitsOption = dataReader.getDataRecords(None)
      assert(splitsOption.isEmpty)
    }

    it("should return an Option with an empty List when receiving an empty List") {
      val splitsOption = dataReader.getDataRecords(Option(List()))
      assert(splitsOption.isDefined)
      assert(splitsOption.get == List())
    }
  }

  describe("getRecords") {
    val source: Source = Source.fromString(
      """ts id dat00 dat01 dat02
        |ts id dat10 dat11 dat12
      """.stripMargin)

    val columns = List("timestamp", "id", "stream0", "stream1", "stream2")
    val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2, readInRam = false)

    it("should return the data records as long as there are records left") {
      var recordsOption = dataReader.getRecords
      assert(recordsOption.isDefined)
      assert(recordsOption.get == List("dat00", "dat01", "dat02"))

      recordsOption = dataReader.getRecords
      assert(recordsOption.isDefined)
      assert(recordsOption.get == List("dat10", "dat11", "dat12"))
    }

    it("should return 'None' when no more records are left") {
      val recordsOption = dataReader.getRecords
      assert(recordsOption.isEmpty)
    }
  }

  describe("hasRecords") {
    val source: Source = Source.fromString("ts id dat00 dat01 dat02")

    val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2, readInRam = false)

    it("should return true when record is left") {
      assert(dataReader.hasRecords)
    }

    it("should return false when no records are left") {
      dataReader.getRecords
      assert(!dataReader.hasRecords)
    }
  }
}
