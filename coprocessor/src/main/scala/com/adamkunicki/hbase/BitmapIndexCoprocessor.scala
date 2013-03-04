package com.adamkunicki.hbase

import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, ObserverContext, BaseRegionObserver}
import org.apache.hadoop.hbase.regionserver.InternalScanner
import java.util
import org.apache.hadoop.hbase.client.Result
import com.googlecode.javaewah.EWAHCompressedBitmap
import org.apache.hadoop.hbase.KeyValue
import java.lang.Integer
import com.adamkunicki.util.Logging

class BitmapIndexCoprocessor extends BaseRegionObserver with Logging {
  override def preScannerNext(e: ObserverContext[RegionCoprocessorEnvironment],
                              s: InternalScanner, results: util.List[Result],
                              limit: Int,
                              hasMore: Boolean): Boolean = {
    // Skip default behavior: aka. returning the requested row
    e.bypass()
    // Do our own scanning of the requested row in order to return a bitmap

    // Is there work to do?
    if (!hasMore) {
      return false
    }

    val bitmapKeyValue:Array[KeyValue] = new Array[KeyValue](1)
    bitmapKeyValue(0) = nextKeyValue(s)
    results.add(new Result(bitmapKeyValue))

    hasMore
  }

  override def postScannerNext(e: ObserverContext[RegionCoprocessorEnvironment],
                               s: InternalScanner,
                               results: util.List[Result],
                               limit: Int,
                               hasMore: Boolean): Boolean = {
    hasMore
  }

  private def nextKeyValue(s: InternalScanner): KeyValue = {
    val currentRow: util.List[KeyValue] = new util.ArrayList[KeyValue](1) // Only expecting one value
    // Get the current KeyValue for the row
    s.next(currentRow)
    val bitmap = new EWAHCompressedBitmap()

    def getPartialKey_ROW_COLF(kv: KeyValue) = {
      kv.getKey.slice(kv.getKeyOffset, kv.getQualifierOffset - 1)
    }

    val goldenKeyValue: KeyValue = currentRow.get(0)
    log.debug("Golden KV: " + goldenKeyValue)

    val goldenKey: Array[Byte] = getPartialKey_ROW_COLF(currentRow.get(0))
    log.debug("Golden Key: " + new String(goldenKey))

    def processNextRow(s: InternalScanner) {
      if (s.next(currentRow)) {
        log.debug("Current KV: " + currentRow.get(0))

        if (getPartialKey_ROW_COLF(currentRow.get(0)).equals(goldenKey)) {
          val bitmapIndex: Int = Integer.parseInt(new String(currentRow.get(0).getValue))
          bitmap.set(bitmapIndex)
          processNextRow(s)
        }
      }
      log.debug("Done processing.")
    }

    new KeyValue(goldenKeyValue.getRow, goldenKeyValue.getFamily, "bitmap".getBytes, BitmapUtils.toBytes(bitmap))
  }
}
