package com.creditkarma.blink.instrumentation

import java.text.SimpleDateFormat
import java.util.Date

import com.creditkarma.blink.base._
import com.creditkarma.blink.impl.spark.exporter.kafka.KafkaExportMeta
import net.minidev.json.JSONObject
import net.minidev.json.JSONValue


/**
  * Created by shengwei.wang on 12/8/16.
  * TO-DO: 1) follow metrics schema
  *        2) aggregated raw data
  *
  *{
   "nDateTime": "1997-07-16T19:20:30.451+16:00", // timestamp with milliseconds
   "dataSource": <dataPipeline>_<In|Out>, // component service name
   "eventType": TableTopics_g0_t01_p[0|1|2]_DPMetrics, // TableTopic schema
   "nPDateTime": "1997-07-16T19:00:00.000+16:00",
   "payload": {
      "inRecords": "integer",
      "outRecords": "integer",
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
  *
  */
class InfoToKafkaInstrumentor(flushInterval:Long,host:String,port:String,topicName:String,sessionTo:String) extends Instrumentor{
  private val singleWriter = new InfoToKafkaSingleThreadWriter(host,port,topicName,sessionTo)
  private var dataBuffered:scala.collection.mutable.MutableList[String] = new scala.collection.mutable.MutableList[String]
  private val startTime:Long =  java.lang.System.currentTimeMillis()

  private var prevFlashTime:Long = startTime
  private var cycleStart = 0L
  private var cycleEnd = 0L
  private var hasUpdates = false
  private var numberOfCPFailures = 0
  private var numberOfWriteFailures = 0
  private var totalInRecords = 0L
  private var totalOutRecords = 0L

  private val metricsTemplate = """{"nDateTime": "", "dataSource": "", "eventType": "", "nPDateTime": "", "payload": "",  "ipAddress":"", "userAgent": "", "geoLocLat":"",   "geoLocLong": "" }"""
  private val jsonObject:JSONObject  = JSONValue.parse(metricsTemplate).asInstanceOf[JSONObject]
  private val payloadTemplate = """{"inRecords": "", "outRecords": "", "RecordsErrReason": "", "tsEvent": "", "elapsedTime": "" }"""
  private val payloadObject:JSONObject  = JSONValue.parse(payloadTemplate).asInstanceOf[JSONObject]

  // a clousre to flush and reset
  def flush:Unit = {
    val currentTs = java.lang.System.currentTimeMillis()
    if((currentTs - prevFlashTime >= flushInterval) ){

      val format:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      jsonObject.put("nDateTime",format.format(new Date(java.lang.System.currentTimeMillis())))
      jsonObject.put("dataSource", "Blink_In")
      jsonObject.put("eventType", "TableTopics_g0_t01_p0_DPMetrics")
      jsonObject.put("nPDateTime", format.format(new Date(java.lang.System.currentTimeMillis())))
      jsonObject.put("ipAddress", "123.123.123.123")
      payloadObject.put("inRecords",totalInRecords.toString)
      payloadObject.put("outRecords",totalOutRecords.toString)
      payloadObject.put("elapsedTime",(java.lang.System.currentTimeMillis()-cycleStart).toString)
      payloadObject.put("tsEvent",java.lang.System.currentTimeMillis().toString)
      if(hasUpdates)
      payloadObject.put("RecordsErrReason","there are " + numberOfCPFailures + " checkpoint failures and " + numberOfWriteFailures + " write failures" )
      else
        payloadObject.put("RecordsErrReason","there are no failures in this minute!" )
      jsonObject.put("payload",payloadObject.toJSONString())
      dataBuffered += jsonObject.toJSONString()

      singleWriter.saveBlockToKafka(dataBuffered)
      dataBuffered = new scala.collection.mutable.MutableList[String]
      prevFlashTime = currentTs
      hasUpdates = false
      numberOfCPFailures = 0
      numberOfWriteFailures = 0
      totalInRecords = 0L
      totalOutRecords = 0L
   }
  }

  override def name: String = this.getClass.getName

  var cycleId: Long = 0

  override def cycleStarted(module: CoreModule): Unit = {
    cycleStart = java.lang.System.currentTimeMillis()
  }

  override def cycleCompleted(module: CoreModule): Unit = {
    cycleEnd = java.lang.System.currentTimeMillis()
    flush
    cycleId += 1
  }

  override def updateStatus(module: CoreModule, status: Status): Unit = {
    if(module.moduleType == ModuleType.Core && status.message == "checkpoint commit failure" ){
      numberOfCPFailures += 1
      hasUpdates = true
    }
    if(status.statusCode == StatusCode.FATAL){
      flush
      System.exit(0)
    }
  }

  override def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    if(module.moduleType == ModuleType.Writer){

        for(tempOSR <- metrics.asInstanceOf[KafkaExportMeta].allOffsetRanges){
          totalInRecords += tempOSR.count()
        }

        for(tempOSR <- metrics.asInstanceOf[KafkaExportMeta].completedOffsetRanges){
          totalOutRecords += tempOSR.count()
        }

      if(totalInRecords > totalOutRecords) {
        hasUpdates = true
        numberOfWriteFailures += 1
      }

    }
  }

  override def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
  }

  override def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
  }




}
