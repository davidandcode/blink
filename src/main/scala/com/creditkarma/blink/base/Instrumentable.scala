package com.creditkarma.blink.base

/**
  * Created by yongjia.wang on 11/17/16.
  */
trait Instrumentable {

  private val _instrumentors: scala.collection.mutable.Map[String, Instrumentor] = scala.collection.mutable.Map.empty

  private[base] def instrumentors = _instrumentors.values

  def registerInstrumentor(instrumentor: Instrumentor): Unit = {
    _instrumentors.get(instrumentor.name) match {
      case Some(ins) =>
        throw new Exception(
          s"Instrumentor with the same name already registered: ${instrumentor}\n" +
            s"Existing: ${ins.getClass.getCanonicalName}\n" +
            s"New: ${instrumentor.getClass.getCanonicalName}")
      case None =>
        _instrumentors += instrumentor.name -> instrumentor
    }
  }

  def updateStatus(module: CoreModule, status: Status): Unit = {
    instrumentors.foreach(_.updateStatus(module, status))
  }

  def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    instrumentors.foreach(_.updateMetrics(module, metrics))
  }

  def cycleStarted(module: CoreModule): Unit = {
    instrumentors.foreach(_.cycleStarted(module))
  }

  def cycleCompleted(module: CoreModule): Unit = {
    instrumentors.foreach(_.cycleCompleted(module))
  }

  def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
    instrumentors.foreach(_.phaseStarted(module, phase))
  }

  def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
    instrumentors.foreach(_.phaseCompleted(module, phase))
  }
}
