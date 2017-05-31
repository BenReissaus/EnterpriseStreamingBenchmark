package org.hpi.esb.datasender.output

import java.io.File

import com.github.tototoshi.csv.CSVReader
import org.hpi.esb.commons.output.ValueFormatter.round
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config.ConfigHandler
import org.hpi.esb.datasender.output.OutputHelper._


class BenchmarkRunResultMerger extends ResultMerger with Logging {

  val outputFileName = s"${BenchmarkRunResultMerger.benchmarkRunResultPrefix}_${ConfigHandler.resultFileNamePrefix()}"

  def execute(): Unit = {
    val prefix = ConfigHandler.resultFileNamePrefix()
    val resultFiles = filterSameRunFiles(getListOfFiles(ConfigHandler.resultsPath), prefix)
    if(resultFiles.nonEmpty) {
      val readers = resultFiles.map(CSVReader.open)

      // get header and first row of result files
      val runResults: List[Map[String, String]] = readers.map(r => r.allWithHeaders().head)
      val mergedRunResults: List[List[String]] = mergeRunResults(runResults)

      writeResults(mergedRunResults, outputFileName)

    } else {
      logger.info("No benchmark run result files were found for merger.")
    }
  }

  /**
    * All benchmark runs share the same config values. They are copied as they are.
    * For the result values averages are calculated.
    * @param results
    * @return a list of lists/rows
    */
  def mergeRunResults(results: List[Map[String, String]]): List[List[String]] = {
    val configAttributes: Map[String, String] = extractConfigAttributes(results)
    val resultValues: List[Map[String, String]] = extractResultValues(results, configAttributes)
    val averageResultValues = getAverageOfRunResultValues(resultValues)

    val (configKeys, configValues) = getSortedKeyValueLists[String](configAttributes)
    val (mergedResultKeys, mergedResultValues) = getSortedKeyValueLists[String](averageResultValues)

    List(configKeys ++ mergedResultKeys, configValues ++ mergedResultValues)
  }

  def extractConfigAttributes(results: List[Map[String, String]]): Map[String, String] = {
    val configValues = ConfigHandler.getImportantConfigValues(ConfigHandler.getConfig())

    val allSameConfigValues = results.forall(result => configValues.forall { case (key, value) => result(key) == value })
    if (!allSameConfigValues) {
      logger.error("All benchmark runs must have the same config values")
      sys.exit(1)
    } else {
      configValues
    }
  }

  def extractResultValues(results: List[Map[String, String]], configValues: Map[String, String]): List[Map[String, String]] = {
    results.map(result => result.filterKeys(key => !configValues.contains(key)))
  }

  def getAverageOfRunResultValues(resultValues: List[Map[String, String]]): Map[String, String] = {
    val resultValuesAsLists = resultValues.map(resultValue => resultValue.map { case (key, value) => key -> List[String](value) })

    val concatenated = resultValuesAsLists.tail.foldLeft(resultValuesAsLists.head) { case (acc, r) => concatMapValues(acc, r) }

    val merged = concatenated.map { case (key, values) => key -> getAverage(values) }
    merged
  }

  def getAverage(l: List[String]): String = {
    val avg = average(l.map(_.toDouble))
    val rounded = round(avg, precision = 2)
    f"$rounded%1.0f"
  }

  def average(l: List[Double]): Double = {
    l.sum / l.length
  }

  def filterSameRunFiles(files: List[File], prefix: String): List[File] = {
    this.filterFilesByPrefix(files, prefix)
  }
}

object BenchmarkRunResultMerger extends Logging {
  val benchmarkRunResultPrefix = "Run_Merged"

  def main(args: Array[String]): Unit = {
    val merger = new BenchmarkRunResultMerger()
    merger.execute()
  }
}
