package com.creditkarma.blink.instrumentation

import com.creditkarma.blink.base._
import net.minidev.json.JSONObject
import net.minidev.json.JSONValue
import org.apache.kafka.common.utils.SystemTime;

/**
  * Created by shengwei.wang on 12/8/16.
  * TO-DO: 1) follow metrics schema
  *        2) aggregated raw data
  *
  *
  *        {
   "nDateTime": "1997-07-16T19:20:30.451+16:00", // timestamp with milliseconds
   "dataSource": <dataPipeline>_<In|Out>, // component service name
   "eventType": TableTopics_g0_t01_p[0|1|2]_DPMetrics, // TableTopic schema
   "nPDateTime": "1997-07-16T19:00:00.000+16:00",
   "payload": {

      "nRecords": "integer",
      "nRecordsErr": "integer",
      "RecordsErrReason": "string",
      "tsEvent": "integer",
      "elapsedTime": "integer"
   },
   "ipAddress": 3467216389, // IP Address of instance
   "userAgent": "instance Id", // Instance ID or hostname of instance
   "geoLocLat": 43.546, // Not used
   "geoLocLong": 34.443 // Not used
}
  *
  */
class InfoToKafkaInstrumentor(flushInterval:Long,flushSize:Int,host:String,port:String,topicName:String) extends Instrumentor{
  private val singleWriter = new InfoToKafkaSingleThreadWriter(host,port,topicName)
  private var dataBuffered:scala.collection.mutable.MutableList[String] = new scala.collection.mutable.MutableList[String]
  private val startTime:Long =  java.lang.System.currentTimeMillis()

  private val startTemplate = "{\"nDateTime\": \"\",\"dataSource\": \"\",\"eventType\": \"\",\"nPDateTime\": \"\",\"payload\": {\"cycleId\": \"\",\"cycleStatus\": \"started\"},\"ipAddress\": \"\", \"userAgent\": \"\", \"geoLocLat\": \"\" , \"geoLocLong\":  \"\"}"
  val startJsonObject:JSONObject  = JSONValue.parse(startTemplate).asInstanceOf[JSONObject]

  private val metricsTemplate = "{\"nDateTime\": \"\",\"dataSource\": \"\",\"eventType\": \"\",\"nPDateTime\": \"\",\"payload\": {\"cycleId\": \"\",\"cycleStatus\": \"\"},\"ipAddress\": \"\", \"userAgent\": \"\", \"geoLocLat\": \"\" , \"geoLocLong\":  \"\"}"
  val jsonObject:JSONObject  = JSONValue.parse(metricsTemplate).asInstanceOf[JSONObject]

  private val payloadTemplate = ""
  val payLoadJsonObject:JSONObject  = JSONValue.parse(payloadTemplate).asInstanceOf[JSONObject]


  // a clousre to flush and reset
  def flush:Unit = if((java.lang.System.currentTimeMillis() - startTime) >= startTime) {
    singleWriter.saveBlockToKafka(dataBuffered)
    dataBuffered = new scala.collection.mutable.MutableList[String]

  }

  override def name: String = this.getClass.getName

  var cycleId: Long = 0
  override def cycleStarted(module: CoreModule): Unit = {
    val systemTs = java.lang.System.currentTimeMillis()
    startJsonObject.put("nDateTime",s"$systemTs")
    startJsonObject.get("payload").asInstanceOf[JSONObject].put("cycleId",s"$cycleId")
    dataBuffered +=(startJsonObject.toJSONString)
  }

  override def cycleCompleted(module: CoreModule): Unit = {
    dataBuffered +=(s"${module.portalId} Cycle $cycleId completed")
    flush
    cycleId += 1
  }

  override def updateStatus(module: CoreModule, status: Status): Unit = {
    dataBuffered += s"${module.portalId} Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), status=${status}"
    if(status.statusCode == StatusCode.FATAL){
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
  }

  override def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
    dataBuffered += s"${module.portalId} Cycle=$cycleId, ${phase} phase started"
  }

  override def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
    dataBuffered += s"${module.portalId} Cycle=$cycleId, ${phase} phase completed"
  }


}
