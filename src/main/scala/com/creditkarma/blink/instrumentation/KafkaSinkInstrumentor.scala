package com.creditkarma.blink.instrumentation

import java.net._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.creditkarma.blink.base._
import net.minidev.json.{JSONObject, JSONValue}
import org.apache.commons.lang3.time.DateUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
  * Created by shengwei.wang on 12/8/16.
  *
  * {
  * "nDateTime": "1997-07-16T19:20:30.451+16:00", // timestamp with milliseconds
  * "dataSource": <dataPipeline>_<In|Out>, // component service name
  * "eventType": TableTopics_g0_t01_p[0|1|2]_DPMetrics, // TableTopic schema
  * "nPDateTime": "1997-07-16T19:00:00.000+16:00",
  * "payload": {
  * "nRecords": "integer",
  * "nRecordsErr": "integer",
  * "RecordsErrReason": "string",
  * "tsEvent": "integer",
  * "elapsedTime": "integer"
  * },
  * "ipAddress": 3467216389, // IP Address of instance
  * "userAgent": "instance Id", // Instance ID or hostname of instance
  * "geoLocLat": 43.546, // Not used
  * "geoLocLong": 34.443 // Not used
  * }
  *
  *
  */
class KafkaSinkInstrumentor(flushInterval: Long, host: String, port: String, topicName: String, sessionTo: String) extends Instrumentor {
  val kafkaWriter = new KafkaWriter()
  private var dataBuffered: scala.collection.mutable.MutableList[String] = new scala.collection.mutable.MutableList[String]
  private val startTime: Long = java.lang.System.currentTimeMillis()
  private var portalId = ""

  private var prevFlashTime: Long = startTime
  private var cycleStart = 0L
  private var cycleEnd = 0L
  private var hasUpdates = false
  private var numberOfCPFailures = 0
  private var numberOfWriteFailures = 0
  private var totalInRecords = 0L
  private var totalOutRecords = 0L

  private val metricsTemplate = """{"nDateTime": "", "dataSource": "", "eventType": "", "nPDateTime": "", "payload": "",  "ipAddress":"", "userAgent": "", "geoLocLat":"",   "geoLocLong": "" }"""
  private val jsonObject: JSONObject = JSONValue.parse(metricsTemplate).asInstanceOf[JSONObject]
  private val payloadTemplate = """{"nRecords": "", "nRecordsErr": "", "RecordsErrReason": "", "tsEvent": "", "elapsedTime": "" }"""
  private val payloadObject: JSONObject = JSONValue.parse(payloadTemplate).asInstanceOf[JSONObject]

  // a clousre to flush and reset
  def flush: Unit = {
    val currentTs = java.lang.System.currentTimeMillis()
    if ((currentTs - prevFlashTime >= flushInterval)) {

      val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      jsonObject.put("nDateTime", format.format(new Date(currentTs)))
      jsonObject.put("dataSource", "Blink_Out")
      jsonObject.put("eventType", "TableTopics_g0_t01_p0_DPMetrics")
      jsonObject.put("nPDateTime", format.format(DateUtils.truncate(new Date(currentTs), Calendar.HOUR)))
      jsonObject.put("ipAddress", InetAddress.getLocalHost.getHostAddress)
      jsonObject.put("userAgent", portalId)
      payloadObject.put("nRecords", totalInRecords.toString)
      payloadObject.put("nRecordsErr", (totalInRecords-totalOutRecords).toString)
      payloadObject.put("elapsedTime", ((currentTs - prevFlashTime)/1000).toString)
      payloadObject.put("tsEvent", (currentTs/1000).toString)
      if (hasUpdates)
        payloadObject.put("RecordsErrReason", s"there are ${numberOfCPFailures} checkpoint failures and ${numberOfWriteFailures} write failures.")
      else
        payloadObject.put("RecordsErrReason", "there are no failures in this minute!")
      jsonObject.put("payload", payloadObject.toJSONString())
      dataBuffered += jsonObject.toJSONString()

      kafkaWriter.saveBlockToKafka(dataBuffered)
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
    // need a better way to identify status
    if (module.moduleType == ModuleType.Core && status.message == "checkpoint commit failure") {
      numberOfCPFailures += 1
      hasUpdates = true
    }
    if (status.statusCode == StatusCode.FATAL) {
      flush
      System.exit(0)
    }
  }

  override def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    if (module.moduleType == ModuleType.Writer) {
      portalId = module.portalId
      totalInRecords += metrics.asInstanceOf[ExporterMetrics].inRecords
      totalOutRecords += metrics.asInstanceOf[ExporterMetrics].outRecords

      if (totalInRecords > totalOutRecords) {
        hasUpdates = true
        numberOfWriteFailures += 1
      }

    }
  }

  override def phaseStarted(module: CoreModule, phase: Phase.Value): Unit = {
  }

  override def phaseCompleted(module: CoreModule, phase: Phase.Value): Unit = {
  }

  /**
    * Created by shengwei.wang on 12/8/16.
    * This writer is designed to run in a single thread and it is NOT for general purpurse but only for instrumentation.
    */
  class KafkaWriter {
    val keyString = "INSTRUMENTATION"
    val props = new Properties()
    props.put("bootstrap.servers", host + ":" + port) //props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("session.timeout.ms", sessionTo);

    val producer = new KafkaProducer[String, String](props)

    def saveBlockToKafka(blockData: Seq[String]): Unit = {
      for (eachLine <- blockData) {
        val record = new ProducerRecord(topicName, keyString, eachLine)
        producer.send(record)
      }
    }

  }

}


