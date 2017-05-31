package org.hpi.esb.datasender.output

import java.io.File

import com.github.tototoshi.csv.CSVReader
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config.ConfigHandler
import org.hpi.esb.datasender.output.OutputHelper._

class BenchmarkScenarioResultMerger extends ResultMerger {

  val outputFileName = s"${BenchmarkScenarioResultMerger.benchmarkScenarioResultPrefix}"

  override def execute(): Unit = {
    val resultMaps = getAllRunResults()

    val resultValuesAsLists = resultMaps.map(resultMap => resultMap.map { case(key, value) => key -> List[String](value)})
    val concatenated = resultValuesAsLists.tail.foldLeft(resultValuesAsLists.head) { case (acc, r) => concatMapValues(acc, r)}

    val (resultKeys, resultValuesAsList) = getSortedKeyValueLists[List[String]](concatenated)

    val results = resultKeys :: resultValuesAsList.transpose

    writeResults(results, outputFileName)
  }

  def filterSameScenarioFiles(files: List[File], prefix: String): List[File] = {
    this.filterFilesByPrefix(files, prefix)
  }

  def getAllRunResults(): List[Map[String, String]] = {
    val prefix = BenchmarkRunResultMerger.benchmarkRunResultPrefix
    val files = getListOfFiles(ConfigHandler.resultsPath)
    val scenarioResultFiles = filterSameScenarioFiles(files, prefix)
    val readers = scenarioResultFiles.map(CSVReader.open)

    readers.map(r => r.allWithHeaders().head)
  }
}

object BenchmarkScenarioResultMerger extends Logging {
  val benchmarkScenarioResultPrefix = "Scenario_Merged"
  def main(args: Array[String]): Unit = {
    val merger = new BenchmarkScenarioResultMerger()
    merger.execute()
  }
}
