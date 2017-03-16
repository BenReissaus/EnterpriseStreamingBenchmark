package org.hpi.esb.config

case class DataReaderConfig(columns: Option[List[String]], dataColumnStart: Option[Int],
                            columnDelimiter: Option[String], dataInputPath: Option[String]) extends Configurable {

  def isValid: Boolean = {
    areValidColumns() && isValidDataColumnStart() && isValidColumnDelimiter() &&
      isValidDataInputPath(dataInputPath)
  }

  def areValidColumns(): Boolean = {
    def valid = columns.isDefined && columns.get.nonEmpty

    if (!valid) {
      logger.error("You must specify at least one column for the data input file.")
    }
    valid
  }

  def isValidDataColumnStart(): Boolean = {
    val valid = dataColumnStart.isDefined && (0 <= dataColumnStart.get) &&
      (dataColumnStart.get < columns.get.size)

    if (!valid) {
      logger.error(s"Invalid config: 'dataColumnStart' must be defined and it must be >= 0 and < size of column list")
    }
    valid
  }

  def isValidColumnDelimiter(): Boolean = {
    checkAttributeOptionHasValue("column delimiter", columnDelimiter)
  }


  override def toString(): String = {
    val prefix = "datasender.dataModel"
    s"""
       |$prefix.columns = ${opToStr(columns)}
       |$prefix.columnStart = ${opToStr(dataColumnStart, "0")}
        """.stripMargin
  }
}

