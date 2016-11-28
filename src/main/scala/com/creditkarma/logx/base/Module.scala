package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Module extends Instrumentable {
  def moduleType: ModuleType.Value

  def updateStatus(status: Status): Unit = {
    updateStatus(this, status)
  }

  def updateMetrics(metrics: Metrics): Unit = {
    updateMetrics(this, metrics)
  }
}

object ModuleType extends Enumeration {
  val Core, Reader, Writer, Transformer, CheckpointService = Value
}
