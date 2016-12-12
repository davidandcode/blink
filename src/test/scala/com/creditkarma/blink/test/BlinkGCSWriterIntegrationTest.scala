package com.creditkarma.blink.test

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.factory.KafkaExportWorkerCreator
import com.creditkarma.blink.impl.spark.exporter.kafka.gcs.{GCSSubPartition, GCSWriter}
import org.apache.log4j.{Level, LogManager}

/**
  * Created by shengwei.wang on 12/9/16.
  */
object BlinkGCSWriterIntegrationTest extends KafkaIntegrationTest[String, String]{

  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))
   def main(args: Array[String]) {

    // LogManager.getLogger("org.apache").setLevel(Level.WARN)
   //  LogManager.getLogger("kafka").setLevel(Level.WARN)
startKafka()
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2014-05-07T14:22:35.863513-08:00\"}")
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-11-27T14:22:35.863513-08:00\"}")
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2019-05-07T14:22:35.863513-08:00\"}")
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"20196-05-07T14:22:35.863513-08:00\"}")
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"20a19-05-07T14:22:35.863513-08:00\"}")
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"tsfad\":\"2016-12-11T14:22:35.863513-08:00\"}")
     sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863513-08:00\"}")
     val portalId = MainApp.castPortal("src/test/resources/kafka.test3.properties")
shutDownKafka()

    }
}


class GCSTestWriterCreator extends KafkaExportWorkerCreator[String,String,GCSSubPartition] {
  override def writer: GCSWriter = {

    val tsName = getOrFail("tsName")
    val ifWithMirco = getOrFail("ifWithMicro").toBoolean
    val enforcedFields = getOrFail("enforcedFields")
    val credentialsPath = get("credentialsPath").getOrElse("")
    val connectTimeoutMs = get("connectTimeoutMs").getOrElse("10000").toInt
    val readTimeoutMs = get("readTimeoutMs").getOrElse("10000")toInt
    val bucketName = get("bucketName").getOrElse("")
    val outputAppString = get("outputAppString").getOrElse("")
    val metaData = get("metaData").getOrElse("")
    val cacheControl = get("cacheControl").getOrElse("")
    val outputFileExtension = get("outputFileExtension").getOrElse("")

    new GCSWriter(
      tsName,
      ifWithMirco,
      enforcedFields,
      credentialsPath,
      connectTimeoutMs,
      readTimeoutMs,
      bucketName,
      outputAppString,
      metaData,
      cacheControl,
      outputFileExtension
    )



  }
}