package com.creditkarma.blink.instrumentation
import com.creditkarma.blink.base._
import com.creditkarma.blink.utils.LazyLog
import org.apache.log4j._

/**
  * Created by yongjia.wang on 11/17/16.
  */
class LogInfoToFileInstrumentor(maxFileSize:String, MaxBackupIndex:String, fName:String, pattern:String) extends Instrumentor with LazyLog {

  def printLogToFileSetUp():Unit = {
    val mAppender = new RollingFileAppender()
    mAppender.setMaxFileSize(maxFileSize)
    mAppender.setMaxBackupIndex(MaxBackupIndex.toInt)
    mAppender.setFile(fName)
    mAppender.setLayout(new PatternLayout(pattern))
    mAppender.activateOptions()
    logger.addAppender(mAppender)
  }

  def setLevel(level: Level): Unit = {
    logger.setLevel(level)
  }

  setLevel(Level.INFO)
  printLogToFileSetUp()

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

object LogInfoToFileInstrumentor {
  def apply(maxFileSize:String, MaxBackupIndex:String, fName:String, pattern:String): LogInfoToFileInstrumentor = new LogInfoToFileInstrumentor(maxFileSize, MaxBackupIndex, fName, pattern)
}
