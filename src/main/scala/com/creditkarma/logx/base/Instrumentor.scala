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

  def updateStatus(module: Module, status: Status): Unit

  def updateMetric(module: Module, metrics: Map[MetricArgs.Value, Any] = Map.empty): Unit

}
