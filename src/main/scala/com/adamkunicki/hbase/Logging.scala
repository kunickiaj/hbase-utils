package com.adamkunicki.hbase

import org.slf4j.LoggerFactory

trait Logging {

  lazy val log = LoggerFactory.getLogger(getClass)

}
