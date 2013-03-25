package com.adamkunicki.hbase

import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, BaseEndpointCoprocessor}
import com.googlecode.javaewah.EWAHCompressedBitmap
import org.apache.hadoop.hbase.client.Scan
import java.util
import org.apache.hadoop.hbase.KeyValue
import com.adamkunicki.util.Logging
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._
import scala.Predef._

class BitmapIndexEndpointCoprocessor extends BaseEndpointCoprocessor with BitmapIndexProtocol with Logging {
  def matchesTerms(terms: Seq[String], matchFunc: (EWAHCompressedBitmap, EWAHCompressedBitmap) => EWAHCompressedBitmap): EWAHCompressedBitmap = {
    log.debug("Entered coprocessor!")

    var bitmaps: List[EWAHCompressedBitmap] = List.empty

    // Scan for each term
    for (term <- terms) {
      log.debug("Scanning term: " + term)
      val scan = new Scan()
      scan.addFamily(Constants.TERM_INT.getBytes)
      scan.setStartRow(Bytes.toBytes(term))
      scan.setStopRow(Bytes.toBytes(term + '\0'))

      val env = getEnvironment.asInstanceOf[RegionCoprocessorEnvironment]
      val scanner = env.getRegion.getScanner(scan)

      val bitmap = new EWAHCompressedBitmap()
      try {
        val currentResults = new util.ArrayList[KeyValue]()
        var hasMore = true
        do {
          hasMore = scanner.next(currentResults)

          for (result <- currentResults.asScala) {
            val bitmapIndex: Int = Integer.parseInt(new String(result.getValue))
            bitmap.set(bitmapIndex)
            log.debug("Setting bit for document: " + bitmapIndex)
          }
          log.debug("Is more work?" + hasMore)
        } while (hasMore)
      } finally {
        scanner.close()
        bitmaps ::= bitmap
      }
    }

    var finalBitmap = bitmaps(0)
    log.debug("Initial final bitmap")
    for (idx <- 1 until bitmaps.length) {
      finalBitmap = matchFunc(finalBitmap, bitmaps(idx))
    }

    finalBitmap
  }
}
