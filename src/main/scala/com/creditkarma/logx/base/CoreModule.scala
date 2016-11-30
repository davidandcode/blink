package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait CoreModule extends Instrumentable {
  def moduleType: ModuleType.Value

  def phaseStarted(phase: Phase.Value): Unit = {
    phaseStarted(this, phase)
  }

  def phaseCompleted(phase: Phase.Value): Unit = {
    phaseCompleted(this, phase)
  }

  def updateStatus(status: Status): Unit = {
    updateStatus(this, status)
  }

  def updateMetrics(metrics: Metrics): Unit = {
    updateMetrics(this, metrics)
  }

  var _portalId: Option[String] = None
  def portalId: String = {
    _portalId match{
      case Some(id) => id
      case None => throw new Exception(s"${moduleType} portal id is not set")
    }
  }
}

object ModuleType extends Enumeration {
  val Core, Reader, Writer, Transformer, CheckpointService = Value
}
