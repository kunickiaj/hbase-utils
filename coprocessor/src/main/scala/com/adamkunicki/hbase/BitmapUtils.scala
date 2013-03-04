package com.adamkunicki.hbase

import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream, ByteArrayOutputStream}
import com.googlecode.javaewah.EWAHCompressedBitmap

object BitmapUtils {
  def toBytes(bitmap: EWAHCompressedBitmap): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    bitmap.serialize(out);
    out.close();
    bytes.toByteArray()
  }

  def fromBytes(bytes: Array[Byte]): EWAHCompressedBitmap = {
    val outBitmap = new EWAHCompressedBitmap()
    val byteStream = new ByteArrayInputStream(bytes)
    val in = new DataInputStream(byteStream)

    outBitmap.deserialize(in)
    in.close()
    outBitmap
  }
}
