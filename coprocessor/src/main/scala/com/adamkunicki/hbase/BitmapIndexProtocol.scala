package com.adamkunicki.hbase

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol
import com.googlecode.javaewah.EWAHCompressedBitmap
import java.io.IOException

trait BitmapIndexProtocol extends CoprocessorProtocol {
  @throws[IOException]("If something goes wrong...")
  def matchesAllTerms(terms: Seq[String]): EWAHCompressedBitmap

  @throws[IOException]("If something goes wrong...")
  def matchesAnyTerms(terms: Seq[String]): EWAHCompressedBitmap
}
