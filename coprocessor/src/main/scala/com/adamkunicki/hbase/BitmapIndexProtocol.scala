package com.adamkunicki.hbase

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol
import com.googlecode.javaewah.EWAHCompressedBitmap
import java.io.IOException

trait BitmapIndexProtocol extends CoprocessorProtocol {
  @throws[IOException]("If something goes wrong...")
  def matchesTerms(terms: Seq[String],
                   matchFunc: (EWAHCompressedBitmap, EWAHCompressedBitmap) => EWAHCompressedBitmap): EWAHCompressedBitmap
}
