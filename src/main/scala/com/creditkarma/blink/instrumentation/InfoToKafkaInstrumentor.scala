package com.creditkarma.blink.instrumentation

import com.creditkarma.blink.base._

/**
  * Created by shengwei.wang on 12/8/16.
  */
class InfoToKafkaInstrumentor(flushSize:Int,host:String,port:String,topicName:String) extends Instrumentor{
  private val singleWriter = new InfoToKafkaSingleThreadWriter(host,port,topicName)
  private var dataBuffered:scala.collection.mutable.MutableList[String] = new scala.collection.mutable.MutableList[String]
  var counter = 0
  // a clousre to flush and reset
  def flush:Unit = if(counter > flushSize){
    singleWriter.saveBlockToKafka(dataBuffered)
    dataBuffered = new scala.collection.mutable.MutableList[String]
    counter = 0
  }

  override def name: String = this.getClass.getName

  var cycleId: Long = 0
  override def cycleStarted(module: CoreModule): Unit = {
    dataBuffered +=(s"${module.portalId} Cycle $cycleId started")
    counter += 1
    flush
  }

  override def cycleCompleted(module: CoreModule): Unit = {
    dataBuffered +=(s"${module.portalId} Cycle $cycleId completed")
    counter += 1
    cycleId += 1
    flush
  }

  override def updateStatus(module: CoreModule, status: Status): Unit = {
    dataBuffered += s"${module.portalId} Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), status=${status}"
    counter += 1
    flush
    if(status.statusCode == StatusCode.Unexpected){
      dataBuffered += "Unexpected situation is encountered, exit now and must have it fixed, to avoid unrecoverable damages"
      flush
      System.exit(0)
    }
  }

  override def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    dataBuffered += (s"${module.portalId} Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), metrics=${
      metrics.metrics.map{
        m => s"[d=${m.dimensions},f=${m.fields}]"
      }.mkString(",")}")
    counter += 1
    flush
  }

  override def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
    dataBuffered += s"${module.portalId} Cycle=$cycleId, ${phase} phase started"
    counter += 1
    flush
  }

  override def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
    dataBuffered += s"${module.portalId} Cycle=$cycleId, ${phase} phase completed"
    counter += 1
    flush
  }


}
