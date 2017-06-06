package org.hpi.esb.datavalidator.output.writers

import java.text.SimpleDateFormat
import java.util.Date

import org.hpi.esb.commons.config.Configs
import org.hpi.esb.commons.output.{CSVOutput, Tabulator}
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datavalidator.configuration.Config.{resultFileName, resultsPath}
import org.hpi.esb.datavalidator.output.model.ConfigValues
import org.hpi.esb.datavalidator.validation.ValidationResult

class ValidatorRunResultWriter extends Logging {

  val currentTime: String = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

  def outputResults(results: List[ValidationResult]): Unit = {

    val configValues = ConfigValues.get(Configs.benchmarkConfig).toList()
    val configValuesHeader = ConfigValues.header

    val resultValues = results.map(_.getMeasuredResults)
    val resultValuesHeader = ValidationResult.getHeader

    val header = configValuesHeader ++ resultValuesHeader
    val values = resultValues.map(resultValueRow => configValues ++ resultValueRow)

    val table = header :: values

    CSVOutput.write(table, resultsPath, resultFileName(currentTime))
    logger.info(Tabulator.format(table))
  }
}
