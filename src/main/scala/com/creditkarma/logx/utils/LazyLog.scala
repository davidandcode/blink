package com.creditkarma.logx.utils
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

  @inline final def trace(message: => String, t: Throwable = null) {
    logger.trace(lazyMessage(message), t)
  }

  @inline final def debug(message: => String, t: Throwable = null) {
    logger.debug(lazyMessage(message), t)
  }

  @inline final def info(message: => String, t: Throwable = null) {
    logger.info(lazyMessage(message), t)
  }

  @inline final def warn(message: => String, t: Throwable = null) {
    logger.warn(lazyMessage(message), t)
  }

  @inline final def error(message: => String, t: Throwable = null) {
    logger.error(lazyMessage(message), t)
  }

  // fatal does not make much sense for logging, although log4j supports it
  @inline final def fatal(message: => String, t: Throwable = null) {
    logger.fatal(lazyMessage(message), t)
  }

  // wire the lazy by-name parameter to the toString method which will be invoked by logger only when passed the level
  private def lazyMessage(message: => String) = new {
    override def toString = message
  }

}