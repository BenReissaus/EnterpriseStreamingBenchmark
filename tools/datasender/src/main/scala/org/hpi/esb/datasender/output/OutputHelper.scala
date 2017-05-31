package org.hpi.esb.datasender.output

import java.io.File

import org.hpi.esb.commons.output.CSVOutput
import org.hpi.esb.datasender.config.ConfigHandler

object OutputHelper {
  def writeResults(mergedResults: List[List[String]], fileName :String): Unit = {
    CSVOutput.write(mergedResults, ConfigHandler.resultsPath, fileName)
  }

  def getSortedKeyValueLists[R](m: Map[String, R]): (List[String], List[R]) = {
    m.map { case (key, value) => (key, value) }.toList
      .sortWith(_._1 < _._1)
      .unzip
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def concatMapValues(m1: Map[String, List[String]], m2: Map[String, List[String]]): Map[String, List[String]] = {
    // convert maps to sequences of tuples and concatenate sequences
    val merged: Seq[(String, List[String])] = m1.toSeq ++ m2.toSeq

    // group by key: (key1 -> Seq((key1, value1), (key1, value2)))
    val grouped: Map[String, Seq[(String, List[String])]] = merged.groupBy(_._1)

    // only keep values
    val accumulatedValues: Map[String, List[List[String]]] = grouped.mapValues(_.map(_._2).toList)

    // merge lists of values
    accumulatedValues.map {
      case (key, value) =>
        key -> value.foldLeft(List.empty[String])((a, b) => a ++ b)
    }
  }
}
