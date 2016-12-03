package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 12/2/16.
  */
trait PortalController {
  def runForever(): Unit
  def runTilCompletion(): Unit
  def fromEarliest(): Unit
  def fromNow(): Unit
}
