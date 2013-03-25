package com.adamkunicki.hbase

import com.adamkunicki.util.{Resources, Logging}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import com.googlecode.javaewah.EWAHCompressedBitmap
import org.scalatest.FunSuite
import com.adamkunicki.hbase.EWAHCompressedBitmapImplicit._

class BitmapIndexCoprocessorTest extends FunSuite with Logging with Resources {

  test("Coprocessor works") {
    val conf = HBaseConfiguration.create()
    val zookeepers = "localhost"
    val tableName = "fx"
    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM, zookeepers)
    conf.set(Constants.ZOOKEEPER_ID_PATH, "/" + tableName + "/ids")
    conf.set(Constants.HBASE_TABLE_NAME, tableName)
    val table = new HTable(conf, tableName)

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
      // Here we're ANDing the bitmaps from each Region as we're interested in matchesAllTerms
      // This will be wrapped up for convenience in the client API.
      bitmap = bitmap.and(it.next().getValue)
    }


    println("Matching keys:")
    val intIds: List[Int] = bitmap
    intIds.map(int => println(int))
  }

}
