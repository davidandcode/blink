package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Module extends Instrumentable {
  def moduleType: ModuleType.Value

  def statusUpdate(status: Status): Unit = {
    instrumentors.foreach(_.updateStatus(this, status))
  }

  def metricUpdate(metrics: Map[MetricArgs.Value, Any]): Unit = {
    instrumentors.foreach(_.updateMetric(this, metrics))
  }
}

object ModuleType extends Enumeration {
  val Core, Reader, Writer, Transformer, CheckpointService = Value
}
