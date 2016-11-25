package com.creditkarma.logx.base

/**
  * This stateless event update interface exposes LogX specific status and metrics
  * [[LogXCore]] already has a lot of the calls inserted
  * Implementations of the modules (reader, writer, etc.) are responsible to report their own custom metrics
  * Implementations of the instrumentor are responsible to interpret and process the metrics
  * It's important the instrumentor itself consumes little resource and all the methods should return immediately (no blocking IO etc.)
  * One example of a simple instrumentor is [[com.creditkarma.logx.instrumentation.LogInfoInstrumentor]]
  */
trait Instrumentor {

  def name: String

  def cycleStarted(): Unit

  def cycleCompleted(): Unit

  def phaseStarted(phase: Phase.Value): Unit

  def phaseCompleted(phase: Phase.Value): Unit

  def updateStatus(module: Module, status: Status): Unit

  /**
    *
    * @param module
    * @param metrics for flexibility, Seq[Map[Any, Any] ] can represent all metrics without introducing another type parameter into the framework
    *                The modules(reader, writer etc.) implementations can chose their specific taxonomy for metrics representation,
    *                and the corresponding instrumentor implementation should understand them.
    */
  def updateMetrics(module: Module, metrics: Seq[Map[Any, Any]]): Unit

}
