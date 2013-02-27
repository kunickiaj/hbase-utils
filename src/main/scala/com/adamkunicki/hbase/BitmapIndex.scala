package com.adamkunicki.hbase

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import java.io.Closeable

trait BitmapIndex extends Closeable {
  protected val conf = HBaseConfiguration.create()
  val table: HTable = createTableHandle

  def createTableHandle: HTable

  def addToIndex(item: Any, terms: Seq[String])

  def getItemsMatchingAllTerms(terms: Seq[String]): Seq[Byte]

  def getItemsMatchingAnyTerms(terms: Seq[String]): Seq[Byte]

  def getItemsMatchingNoTerms(terms: Seq[String]): Seq[Byte]

  def close() {
    table.close()
  }
}
