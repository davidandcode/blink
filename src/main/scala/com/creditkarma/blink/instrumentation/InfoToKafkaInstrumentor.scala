package com.creditkarma.blink.instrumentation

import java.text.SimpleDateFormat
import java.util.Date

import com.creditkarma.blink.base._
import com.creditkarma.blink.impl.spark.exporter.kafka.KafkaExportMeta
import net.minidev.json.JSONObject
import net.minidev.json.JSONValue
import org.apache.spark.streaming.kafka010.OffsetRange

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
class InfoToKafkaInstrumentor(flushInterval:Long,host:String,port:String,topicName:String) extends Instrumentor{
  private val singleWriter = new InfoToKafkaSingleThreadWriter(host,port,topicName)
  private var dataBuffered:scala.collection.mutable.MutableList[String] = new scala.collection.mutable.MutableList[String]
  private val startTime:Long =  java.lang.System.currentTimeMillis()
  private var prevFlashTime:Long = startTime
  private var tic = 0L
  private var tac = 0L

  private val metricsTemplate = "{\"nDateTime\": \"\", \"dataSource\": \"\", \"eventType\": \"\", \"nPDateTime\": \"\", \"payload\": \"\",  \"ipAddress\":\"\", \"userAgent\": \"\", \"geoLocLat\":\"\",   \"geoLocLong\": \"\" }"
  val jsonObject:JSONObject  = JSONValue.parse(metricsTemplate).asInstanceOf[JSONObject]
  private val payloadTemplate = "{\"inRecords\": \"\", \"outRecords\": \"\", \"RecordsErrReason\": \"\", \"tsEvent\": \"\", \"elapsedTime\": \"\" }"
  val payloadObject:JSONObject  = JSONValue.parse(payloadTemplate).asInstanceOf[JSONObject]
  // a clousre to flush and reset
  def flush:Unit = {
    val currentTs = java.lang.System.currentTimeMillis()
    if(currentTs - prevFlashTime >= flushInterval) {
      singleWriter.saveBlockToKafka(dataBuffered)
      dataBuffered = new scala.collection.mutable.MutableList[String]
      prevFlashTime = currentTs
    }
  }

  override def name: String = this.getClass.getName

  var cycleId: Long = 0

  override def cycleStarted(module: CoreModule): Unit = {
    tic = java.lang.System.currentTimeMillis()
  }

  override def cycleCompleted(module: CoreModule): Unit = {
    flush
    cycleId += 1
  }

  override def updateStatus(module: CoreModule, status: Status): Unit = {
    if(module.moduleType == ModuleType.Core && status.message == "checkpoint commit failure" ){
      val oldInfo = payloadObject.get("RecordsErrReason")
      payloadObject.put("RecordsErrReason",oldInfo + s" checkpoint commit failure in blink's ${cycleId} Cycle")
    }
    if(status.statusCode == StatusCode.FATAL){
      flush
      System.exit(0)
    }
  }

  override def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    if(module.moduleType == ModuleType.Writer){
      if(metrics.isInstanceOf[KafkaExportMeta]){
        metrics.asInstanceOf[KafkaExportMeta].outRecords
      }
      val format:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      jsonObject.put("nDateTime",format.format(new Date(java.lang.System.currentTimeMillis())))
      jsonObject.put("dataSource", "Blink_In")

        var totalInRecords = 0L
        for(tempOSR <- metrics.asInstanceOf[KafkaExportMeta].allOffsetRanges){
          totalInRecords += tempOSR.count()
        }
        var totalOutRecords = 0L
        for(tempOSR <- metrics.asInstanceOf[KafkaExportMeta].completedOffsetRanges){
          totalOutRecords += tempOSR.count()
        }
      tac = java.lang.System.currentTimeMillis()
      payloadObject.put("inRecords",totalInRecords.toString)
      payloadObject.put("outRecords",totalOutRecords.toString)
      payloadObject.put("elapsedTime",(tac-tic).toString)
      if(totalInRecords > totalOutRecords)
        payloadObject.put("RecordsErrReason",s"${totalInRecords - totalOutRecords} records lost in writting blink's ${cycleId} Cycle")
      else
        payloadObject.put("RecordsErrReason"," ")
      payloadObject.put("tsEvent",tac.toString)

      jsonObject.put("payload",payloadObject.toJSONString())
      dataBuffered += jsonObject.toJSONString()
    }
  }

  override def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
  }

  override def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
  }


}
