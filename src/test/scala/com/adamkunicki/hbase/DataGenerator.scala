package com.adamkunicki.hbase

import org.scalatest.Ignore
import util.Random

@Ignore
object DataGenerator {

  val terms = List("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K")
  val rand = new Random(System.currentTimeMillis())

  def generateData(numEntries: Int, fn: (String, Seq[String]) => Unit) {
    for (n <- 1 to numEntries) {
      val percentComplete: Int = (n.toFloat / numEntries * 100).round
      if (percentComplete % 5 == 0) {
        updateProgress(percentComplete)
      }
      makeData(n, fn)
    }
  }

  private def updateProgress(percent: Int) {
    val progress = new StringBuilder("|")
    val barsToShow: Int = percent / 5
    for (bars <- 1 to barsToShow) {
      progress.append("=")
    }
    progress.append(">")

    for (bars <- barsToShow to 20) {
      progress.append(" ")
    }
    progress.append("|\r")
    print(progress.toString())
  }

  private def makeData(docId: Int, fn: (String, Seq[String]) => Unit) {
    val numTerms = rand.nextInt(terms.size - 1) + 1
    fn("%010d".format(docId), rand.shuffle(terms).slice(1, numTerms))
  }

}
