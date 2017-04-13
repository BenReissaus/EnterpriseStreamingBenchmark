package org.hpi.esb.datavalidator.output

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.github.tototoshi.csv.CSVWriter
import org.hpi.esb.util.Logging

object CSVOutput extends Logging {

  def write(table: Seq[Seq[Any]], directory: String): Unit = {
    val dir = new File(s"$directory")

    var directoryExists = dir.exists()
    if (!directoryExists) {
      if (dir.mkdir()) {
        directoryExists = true
      } else {
        logger.error("The validation results directory could not be created.")
      }
    }

    if (directoryExists) {
      val currentDate = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
      val f = new File(dir,s"$currentDate.csv")
      val writer = CSVWriter.open(f)
      writer.writeAll(table)
      writer.close()
    }
  }
}
