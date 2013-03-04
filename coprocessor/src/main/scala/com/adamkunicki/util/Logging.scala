package com.adamkunicki.util

import org.slf4j.LoggerFactory

trait Logging {

  lazy val log = LoggerFactory.getLogger(getClass)

}
