package com.adamkunicki.hbase

import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, ObserverContext, BaseRegionObserver}
import org.apache.hadoop.hbase.regionserver.InternalScanner
import java.util
import org.apache.hadoop.hbase.client.Result
import com.googlecode.javaewah.EWAHCompressedBitmap
import org.apache.hadoop.hbase.KeyValue

class BitmapIndexCoprocessor extends BaseRegionObserver {
  override def preScannerNext(e: ObserverContext[RegionCoprocessorEnvironment],
                              s: InternalScanner, results: util.List[Result],
                              limit: Int,
                              hasMore: Boolean): Boolean = {
    // Skip default behavior: aka. returning the requested row
    e.bypass()
    // Do our own scanning of the requested row in order to return a bitmap
    nextKeyValue(s)
    hasMore
  }

  override def postScannerNext(e: ObserverContext[RegionCoprocessorEnvironment],
                               s: InternalScanner,
                               results: util.List[Result],
                               limit: Int,
                               hasMore: Boolean): Boolean = {
    hasMore
  }

  private def nextKeyValue(scanner: InternalScanner): KeyValue = {
    val bitmap = new EWAHCompressedBitmap()
    new KeyValue() // Placeholder
  }
}
