package com.adamkunicki.hbase

import org.scalatest.FunSuite
import java.io.Closeable

class TermIndexTest extends FunSuite {

  def using[S <% Closeable, T](what: S)(block: S => T): T = {
    try {
      block(what)
    }
    finally {
      what.close()
    }
  }

  test("HBase connection is successfully established") {
    using(new TermIndex("192.168.141.155", "FacetIndexTest"))(closable => {
      println("Opened TermIndex")
    })
  }

}
