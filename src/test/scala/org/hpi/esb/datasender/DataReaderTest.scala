package org.hpi.esb.datasender

import java.io.FileNotFoundException
import org.scalatest._
import scalax.file.Path
import scalax.io.{Codec, Resource, Seekable}

class DataReaderTest extends FlatSpec with Matchers with PrivateMethodTester with BeforeAndAfter {

    implicit val codec = Codec.UTF8
    var tmpFile: Path = _
    var dataReader: DataReader = _
    val tmpFilePrefix = "esb_datareader_testfile"
    val tmpFileSuffix = ".tmp"
    val msgEvenLines = "FC"
    val msgOddLines = "Magdeburg"

  before {
    tmpFile = Path.createTempFile(
      prefix = tmpFilePrefix,
      suffix = tmpFileSuffix,
      deleteOnExit = true
    )

    val output: Seekable = Resource.fromFile(tmpFile.path)
    for ( i <- 0 to 10 ) {
      var msg: String = msgOddLines
      if (i % 2 == 0) {
        msg = msgEvenLines
      }
      output.append(s"$msg $i\n")
    }
    dataReader = new DataReader(tmpFile.path)
  }

  after {
  }

  "A DataReader" should "contain a buffered source" in {
    val privateBufferedReader = PrivateMethod[io.BufferedSource]('bufferedSource)
    val bs = dataReader invokePrivate privateBufferedReader()
    assert(bs.isInstanceOf[io.BufferedSource])
  }

  it should "fail w/ FileNotFoundException if path to file is wrong / file does not exist" in {
    assertThrows[FileNotFoundException] {
      new DataReader("/bla/bla.csv")
    }
  }

  it should "need a data input path for initialization" in {
    assertDoesNotCompile("val a = new DataReader()")
  }

  it should "return the correct result when reading lines" in {
    assert(dataReader.getLine.get.equals(s"$msgEvenLines 0"))
    assert(dataReader.getLine.get.equals(s"$msgOddLines 1"))
  }

}
