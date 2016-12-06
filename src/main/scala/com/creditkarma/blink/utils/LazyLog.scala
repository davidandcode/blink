package com.creditkarma.blink.utils
/**
  * Created by yongjia.wang on 11/16/16.
  */
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

// Thin layer on top of log4j to enable delayed log message evaluation
// This relies on the fact that log4j logging methods takes a general object as the argument
// and do not call toString until after checking log level
// One can use more general framework such as slf4j or scala wrapped library such as grizzled_slf4j,
// but this is more efficient with less dependency
trait LazyLog {

  lazy val logger = LogManager.getLogger(this.getClass)

  @inline final def trace(message: => String, t: Throwable) {
    logger.trace(lazyMessage(message), t)
  }
  @inline final def trace(message: => String) {
    logger.trace(lazyMessage(message))
  }

  @inline final def debug(message: => String, t: Throwable) {
    logger.debug(lazyMessage(message), t)
  }
  @inline final def debug(message: => String) {
    logger.debug(lazyMessage(message))
  }

  @inline final def info(message: => String, t: Throwable) {
    logger.info(lazyMessage(message), t)
  }
  @inline final def info(message: => String) {
    logger.info(lazyMessage(message))
  }

  @inline final def warn(message: => String, t: Throwable) {
    logger.warn(lazyMessage(message), t)
  }
  @inline final def warn(message: => String) {
    logger.warn(lazyMessage(message))
  }

  @inline final def error(message: => String, t: Throwable) {
    logger.error(lazyMessage(message), t)
  }
  @inline final def error(message: => String) {
    logger.error(lazyMessage(message))
  }

  // fatal does not make much sense for logging, although log4j supports it
  @inline final def fatal(message: => String, t: Throwable) {
    logger.fatal(lazyMessage(message), t)
  }
  @inline final def fatal(message: => String) {
    logger.fatal(lazyMessage(message))
  }

  // wire the lazy by-name parameter to the toString method which will be invoked by logger only when passed the level
  private def lazyMessage(message: => String) = new {
    override def toString = message
  }
}
