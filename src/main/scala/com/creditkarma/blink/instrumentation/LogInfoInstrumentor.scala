package com.creditkarma.blink.instrumentation
import com.creditkarma.blink.base._
import com.creditkarma.blink.utils.LazyLog
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

import scala.collection.JavaConverters._
/**
  * Created by yongjia.wang on 11/17/16.
  */
class LogInfoInstrumentor extends Instrumentor with LazyLog {

  def printLogToConsole(): Unit = {
    val appenders = LogManager.getRootLogger.getAllAppenders.asScala
    if(!appenders.exists(_.isInstanceOf[ConsoleAppender])){ // if the appender is already at the root, which Spark does by default, don't add it
      val appender = new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"))
      logger.addAppender(appender)
    }
  }
  def setLevel(level: Level): Unit = {
    logger.setLevel(level)
  }

  setLevel(Level.INFO)
  printLogToConsole()

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
    if(status.statusCode == StatusCode.Unexpected){
      fatal("Unexpected situation is encountered, exit now and must have it fixed, to avoid unrecoverable damages")
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
