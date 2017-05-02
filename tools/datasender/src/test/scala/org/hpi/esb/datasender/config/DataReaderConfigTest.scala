package org.hpi.esb.datasender.config

import org.apache.log4j.Logger
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class DataReaderConfigTest extends FlatSpec with Matchers with MockitoSugar {

  val columns: Option[List[String]] = Option(List("col1", "col2", "col3"))
  val dataColumnStart: Option[Int] = Option(1)
  val columnDelimiter: Option[String] = Option("\\s+")
  val dataInputPath: Option[String] = Option("~/hello.world")

  "DataReaderConfig.isValid" should "return false if column list is empty" in {
    val invalidColumns = Option(List[String]())
    val config = DataReaderConfig(invalidColumns, dataColumnStart, columnDelimiter, dataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if 'dataColumnStart' is empty" in {
    val invalidDataColumnStart = None
    val config = DataReaderConfig(columns, invalidDataColumnStart, columnDelimiter, dataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if 'dataColumnStart' is negative" in {
    val invalidDataColumnStart = Option(-1)
    val config = DataReaderConfig(columns, invalidDataColumnStart, columnDelimiter, dataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if 'dataColumnStart' is bigger than 'columns' size" in {
    val tooBigDataColumnStart = Option(10)
    val config = DataReaderConfig(columns, tooBigDataColumnStart, columnDelimiter, dataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if 'columnDelimiter' is empty" in {
    val invalidColumnDelimiter = None
    val config = DataReaderConfig(columns, dataColumnStart, invalidColumnDelimiter, dataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if 'dataInputPath' is empty" in {
    val invalidDataInputPath = None
    val config = DataReaderConfig(columns, dataColumnStart, columnDelimiter, invalidDataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if 'dataInputPath' is not a valid file path" in {
    val invalidDataInputPath = Option("non_existant_file.txt")
    val config = DataReaderConfig(columns, dataColumnStart, columnDelimiter, invalidDataInputPath)
    config.logger = mock[Logger]

    assert(!config.isValid)
    verify(config.logger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if everything is fine" in {
    val config = DataReaderConfig(columns, dataColumnStart, columnDelimiter, dataInputPath)
    config.logger = mock[Logger]
    val mockedConfig = Mockito.spy(config)
    Mockito.doReturn(true, Nil: _*).when(mockedConfig).isValidDataInputPath(dataInputPath)

    assert(mockedConfig.isValid)
    verify(mockedConfig.logger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }
}
