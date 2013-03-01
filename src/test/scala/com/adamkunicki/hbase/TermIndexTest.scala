package com.adamkunicki.hbase

import org.scalatest.FunSuite
import java.io.Closeable

class TermIndexTest extends FunSuite with Logging with Resources {

  test("HBase connection is successfully established") {
    using(new TermIndex("192.168.141.155", "FacetIndexTest"))(index => {
      log.info("Opened TermIndex")
      index.addToIndex("DocA", List("person", "place", "thing"))
    })
  }

}
