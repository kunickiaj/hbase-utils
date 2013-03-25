package com.adamkunicki.hbase

import org.apache.hadoop.hbase.client.{Scan, Put, HTable}
import org.apache.hadoop.hbase.HBaseConfiguration
import java.io.Closeable
import com.adamkunicki.util.{Logging, Resources}
import com.googlecode.javaewah.EWAHCompressedBitmap
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._
import collection.mutable.ListBuffer
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call

trait BitmapIndex extends Closeable with Resources with Logging {
  protected val conf = HBaseConfiguration.create()
  private val table: HTable = createTableHandle
  private val zkUtils = new ZookeeperUtils(conf)

  table.setAutoFlush(true)

  def createTableHandle: HTable

  def addToIndex(item: String, terms: Seq[String]) {
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

  def getItemsMatchingAllTerms(terms: Seq[String]): Seq[String] = {
    val result = table.coprocessorExec(classOf[BitmapIndexProtocol], null, null, new Call[BitmapIndexProtocol, EWAHCompressedBitmap] {
      def call(instance: BitmapIndexProtocol): EWAHCompressedBitmap = {
        instance.matchesTerms(List("A", "B", "F"),
          (bitmap: EWAHCompressedBitmap, otherBitmap: EWAHCompressedBitmap) => bitmap.or(otherBitmap))
      }
    })

    // Iterator represents resulting bitmap from each Region
    val it = result.entrySet().iterator()
    println("Got result iterator")
    var bitmap = it.next().getValue
    while (it.hasNext) {
      println("Got more results")
      // Here we're ANDing the bitmaps from each Region as we're interested in only items that match ALL terms
      bitmap = bitmap.and(it.next().getValue)
    }

    intIdsToItemIds(bitmap)
  }

  def getItemsMatchingAnyTerms(terms: Seq[String]): Seq[String] = List()

  def getItemsMatchingNoTerms(terms: Seq[String]): Seq[String] = List()

  def intIdsToItemIds(bitmap: EWAHCompressedBitmap): Seq[String] = {
    val intIterator = bitmap.intIterator()
    val itemIds = new ListBuffer[String]()

    // TODO Implement batch scanning (perhaps use HTable pool for concurrent scanning)
    while (intIterator.hasNext) {
      val intId = intIterator.next()
      val scan = new Scan()
      scan.addFamily(Constants.INT_ITEM.getBytes)
      val stringId = zkUtils.normalizedInt(intId)
      scan.setStartRow(Bytes.toBytes(stringId))
      scan.setStopRow(Bytes.toBytes(stringId + '\0'))

      val scanner = table.getScanner(scan)

      val iterator = scanner.iterator()
      while (iterator.hasNext) {
        val result = iterator.next()
        log.debug("Got intId of: " + new String(result.value))
        itemIds += new String(result.value)
      }
      scanner.close()
    }
    itemIds.toList
  }

  def close() {
    List(table, zkUtils).map(closeQuietly(_))
  }
}
