package com.creditkarma.blink.instrumentation
import com.creditkarma.blink.base._
import com.creditkarma.blink.utils.LazyLog
/**
  * Created by yongjia.wang on 11/17/16.
  */
class LogInfoInstrumentor extends Instrumentor with LazyLog {

  override def name: String = this.getClass.getName

  var cycleId: Long = 0
  override def cycleStarted(module: CoreModule): Unit = {
    info(s"${module.portalId} Cycle $cycleId started")
  }

  override def cycleCompleted(module: CoreModule): Unit = {
    info(s"${module.portalId} Cycle $cycleId completed")
    cycleId += 1
  }

  override def updateStatus(module: CoreModule, status: Status): Unit = {
    info(s"${module.portalId} Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), status=${status}")
    if(status.statusCode == StatusCode.FATAL){
      fatal("Fatal situation is encountered, exit now and must have it fixed, to avoid unrecoverable damages")
      System.exit(0)
    }
  }

  override def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    info(s"${module.portalId} Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), metrics=${
      metrics.metrics.map{
        m => s"[d=${m.dimensions},f=${m.fields}]"
      }.mkString(",")}")
  }

  override def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
    info(s"${module.portalId} Cycle=$cycleId, ${phase} phase started")
  }

  override def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
    info(s"${module.portalId} Cycle=$cycleId, ${phase} phase completed")
  }
}

object LogInfoInstrumentor {
  def apply(): LogInfoInstrumentor = new LogInfoInstrumentor()
}
