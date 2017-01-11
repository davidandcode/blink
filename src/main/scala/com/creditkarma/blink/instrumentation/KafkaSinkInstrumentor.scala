package com.creditkarma.blink.instrumentation

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.creditkarma.blink.base._
import com.creditkarma.blink.impl.spark.exporter.kafka.KafkaExportMeta
import net.minidev.json.{JSONObject, JSONValue}
import org.apache.commons.lang3.time.DateUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.MutableList

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
class KafkaSinkInstrumentor(flushInterval: Long, host: String, port: String, topicName: String, sessionTimeOutMS: Long) extends Instrumentor {
  val kafkaWriter = new KafkaWriter()

  private var dataBuffered: MutableList[String] = MutableList.empty
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
  private var batchCycles = 0L

  private val metricsTemplate = """{"nDateTime": "", "dataSource": "", "eventType": "", "nPDateTime": "", "payload": "", "ipAddress":"", "userAgent": "", "geoLocLat":"", "geoLocLong": "" }"""
  private val jsonObject: JSONObject = JSONValue.parse(metricsTemplate).asInstanceOf[JSONObject]
  // set constant fields
  jsonObject.put("dataSource", "Blink_Out")
  jsonObject.put("eventType", "TableTopics_g0_t01_p0_DPMetrics")
  jsonObject.put("ipAddress", InetAddress.getLocalHost.getHostAddress)

  private val payloadTemplate = """{"nRecords": "", "nRecordsErr": "", "RecordsErrReason": "", "tsEvent": "", "elapsedTime": "" }"""
  private val payloadObject: JSONObject = JSONValue.parse(payloadTemplate).asInstanceOf[JSONObject]

  def flush(force: Boolean = false): Unit = {
    val currentTs = java.lang.System.currentTimeMillis()

    if (force || (currentTs - prevFlashTime >= flushInterval)) {

      jsonObject.put("userAgent", portalId)
      val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      jsonObject.put("nDateTime", format.format(new Date(currentTs)))
      jsonObject.put("nPDateTime", format.format(DateUtils.truncate(new Date(currentTs), Calendar.HOUR)))
      payloadObject.put("nRecords", totalInRecords.toString)
      payloadObject.put("nRecordsErr", (totalInRecords-totalOutRecords).toString)
      payloadObject.put("elapsedTime", ((currentTs - prevFlashTime)/1000).toString)
      payloadObject.put("tsEvent", (currentTs/1000).toString)
      payloadObject.put("nCycles", batchCycles.toString + " xxxxxx " + cycleId + " XXXXXXXXX " + (currentTs - prevFlashTime >= flushInterval)) // how many blink cycles have passed
      if (hasUpdates){
        payloadObject.put("RecordsErrReason", s"there are ${numberOfCPFailures} checkpoint failures and ${numberOfWriteFailures} write failures.")
      }
      else {
        payloadObject.put("RecordsErrReason", "there are no failures in this minute!")
      }

      jsonObject.put("payload", payloadObject.toJSONString())
      dataBuffered += jsonObject.toJSONString()


      val dest = dataBuffered map {x => x}
      kafkaWriter.saveBlockToKafka(dest)

      //dataBuffered = MutableList.empty
      dataBuffered.clear()

      prevFlashTime = currentTs
      hasUpdates = false
      numberOfCPFailures = 0
      numberOfWriteFailures = 0
      totalInRecords = 0L
      totalOutRecords = 0L
      batchCycles = 0L
    }
  }

  override def name: String = this.getClass.getName

  var cycleId: Long = 0

  override def cycleStarted(module: CoreModule): Unit = {
    cycleStart = java.lang.System.currentTimeMillis()
  }

  override def cycleCompleted(module: CoreModule): Unit = {
    cycleEnd = java.lang.System.currentTimeMillis()
    flush()
    cycleId += 1
  }

  override def updateStatus(module: CoreModule, status: Status): Unit = {
    // need a better way to identify status
    if (module.moduleType == ModuleType.Core && status.message == "checkpoint commit failure") {
      numberOfCPFailures += 1
      hasUpdates = true
    }
    if (module.moduleType == ModuleType.Core && status.message == "closing portal") {
      flush(true)
    }
    if (status.statusCode == StatusCode.FATAL) {
      flush(true)
      System.exit(0)
    }
  }

  override def updateMetrics(module: CoreModule, metrics: Metrics): Unit = {
    if (module.moduleType == ModuleType.Writer) {
      portalId = module.portalId
      //totalInRecords += metrics.asInstanceOf[ExporterMetrics].inRecords
      //totalOutRecords += metrics.asInstanceOf[ExporterMetrics].outRecords

      totalInRecords += metrics.asInstanceOf[KafkaExportMeta].inRecordsUseful
      totalOutRecords += metrics.asInstanceOf[KafkaExportMeta].outRecordsUseful

      batchCycles += 1

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
    val props = new Properties()
    props.put("bootstrap.servers", host + ":" + port) //props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("session.timeout.ms", sessionTimeOutMS.toString);
    //props.put("producer.type", "sync")
    props.put("acks", "all")
    val producer = new KafkaProducer[String, String](props)

    def saveBlockToKafka(blockData: Seq[String]): Unit = {
      for (eachLine <- blockData) {
        val record = new ProducerRecord[String, String](topicName, null, eachLine)
        producer.send(record)
      }

      producer.flush()

      // start a new thread to flush metrics, the caller should not be blocked

      /**
      new Thread(
        new Runnable {
          override def run(): Unit = {
            // just flush the records for now
            producer.flush()
          }
        }
      ).start()

        **/
    }
  }
}


