package com.adamkunicki.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ResultScanner, Scan, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.141.155")

    val table = new HTable(conf, "FacetIndexTest")

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("terms_int"))
    val scanner: ResultScanner = table.getScanner(scan)
    try {
      val it = scanner.iterator()
      while (it.hasNext) {
        val r = it.next()
        println("Row: " + new String(r.getRow))
        println("CF: " + new String(r.getValue(Bytes.toBytes("terms_int"), Bytes.toBytes("0000000001"))))
      }
    } catch {
      case e: Exception =>
        println("There was an exception: " + e.toString)
    }

    println("Done.")
  }

}
