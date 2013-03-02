package com.adamkunicki.hbase

import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.hadoop.hbase.HBaseConfiguration
import java.io.Closeable
import scala.collection.JavaConverters._

trait BitmapIndex extends Closeable with Resources {
  protected val conf = HBaseConfiguration.create()
  private val table: HTable = createTableHandle
  private val zkUtils = new ZookeeperUtils(conf)

  table.setAutoFlush(true)

  def createTableHandle: HTable

  def addToIndex(item: String, terms: Seq[String]) = {
    val intId = zkUtils.getIntIdForRow(item).getBytes

    val intPut = new Put(intId)
    intPut.add(Constants.INT_ITEM.getBytes, item.getBytes, item.getBytes)
    table.put(intPut)

    terms.map(term => {
      val termPut = new Put(term.getBytes)
      termPut.add(Constants.TERM_INT.getBytes, intId, intId)
      termPut.add(Constants.TERM_PLAIN.getBytes, item.getBytes, item.getBytes)
      val itemPut = new Put(item.getBytes)
      itemPut.add(Constants.ITEM_TERM.getBytes, term.getBytes, term.getBytes)
      itemPut.add(Constants.ITEM_INT.getBytes, intId, intId)

      table.put(List(termPut, itemPut).asJava)
    })
  }

  def getItemsMatchingAllTerms(terms: Seq[String]): Seq[String] = List()

  def getItemsMatchingAnyTerms(terms: Seq[String]): Seq[String] = List()

  def getItemsMatchingNoTerms(terms: Seq[String]): Seq[String] = List()

  def close() {
    List(table, zkUtils).map(closeQuietly(_))
  }
}
