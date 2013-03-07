package com.adamkunicki.hbase

import org.scalatest.FunSuite
import com.adamkunicki.util.{Logging, Resources}

class TermIndexTest extends FunSuite with Logging with Resources {

  test("HBase connection is successfully established") {
    using(new TermIndex("localhost", "fx"))(index => {
      log.info("Opened TermIndex")
      DataGenerator.generateData(100, index.addToIndex)
    })
  }

}
