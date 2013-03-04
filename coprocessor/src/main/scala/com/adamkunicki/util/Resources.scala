package com.adamkunicki.util

import java.io.Closeable

trait Resources {
  def using[S <: Closeable, T](what: S)(block: S => T): T = {
    try {
      // Do something with resource
      block(what)
    }
    finally {
      closeQuietly(what)
    }
  }

  def closeQuietly[T <: Closeable](what: T) = {
    scala.util.control.Exception.ignoring(classOf[Exception]) {
      what.close()
    }
  }
}
