package com.adamkunicki.hbase

import com.googlecode.javaewah.EWAHCompressedBitmap

/**
 * Implicit conversions for EWAHCompressedBitmap class
 * For example easily convert to a List[Int] to get a
 * list of all the set bits.
 */
object EWAHCompressedBitmapImplicit {
  implicit def BitmapToList(bitmap: EWAHCompressedBitmap): List[Int] = {
    val builder = List.newBuilder[Int]
    val it = bitmap.intIterator()
    while (it.hasNext) {
      builder += it.next()
    }
    builder.result()
  }
}
