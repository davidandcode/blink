package com.creditkarma.logx.base

import org.spark_project.guava.base.Throwables

/**
  * Created by yongjia.wang on 11/16/16.
  */

trait Status {
  def statusCode: StatusCode.Value
  def message: String
  override def toString: String = s"$statusCode($message)"
}

class StatusOK (msg: =>String) extends Status {
  override val statusCode = StatusCode.OK
  override def message: String = msg
}

class StatusError(val error: Throwable, msg: =>String = "") extends Status {
  val statusCode = StatusCode.ERROR
  override def message: String = s"$msg\n${Throwables.getStackTraceAsString(error)}"
}

class StatusUnexpected (val error: Throwable, msg: =>String) extends Status {
  override val statusCode = StatusCode.Unexpected
  override def message: String = s"$msg\n${Throwables.getStackTraceAsString(error)}"
}

object StatusCode extends Enumeration {
  val OK, ERROR, Unexpected = Value
}
