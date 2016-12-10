package com.creditkarma.blink.test

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.factory.KafkaPartitionGCSWriterCreator
import com.creditkarma.blink.impl.spark.exporter.kafka.ExportWorker
import com.creditkarma.blink.impl.spark.exporter.kafka.gcs.{GCSSubPartition, GCSWriter}

/**
  * Created by shengwei.wang on 12/9/16.
  */
object BlinkGCSWriterIntegrationTest {


   def main(args: Array[String]) {

     val portalId = MainApp.castPortal("src/test/resources/kafka.test2.properties")

val mCreater:GCSTestWriterCreator = new GCSTestWriterCreator()
     val mWriter = mCreater.writer
     val testString: String = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-01-07T14:22:35.863513-08:00\"}"

     val mGCSSubPartition: GCSSubPartition = mWriter.getSubPartition(testString)

     println(mGCSSubPartition.getYear)
     println(mGCSSubPartition.getMonth)
     println(mGCSSubPartition.getDay)
     println(mGCSSubPartition.getHour)
     println(mGCSSubPartition.getMinute)
     println(mGCSSubPartition.getSecond)



    }


}





class GCSTestWriterCreator extends KafkaPartitionGCSWriterCreator {
  override def writer: GCSWriter = {

    val tsName = get("tsName").getOrElse("")
    val ifWithMirco = get("ifWithMicro").getOrElse("false").toBoolean
    val enforcedFields = get("enforcedFields").getOrElse("")
    val credentialsPath = get("credentialsPath").getOrElse("")
    val connectTimeoutMs = get("connectTimeoutMs").getOrElse("10000").toInt
    val readTimeoutMs = get("readTimeoutMs").getOrElse("10000")toInt
    val bucketName = get("bucketName").getOrElse("")
    val outputAppString = get("outputAppString").getOrElse("")
    val metaData = get("metaData").getOrElse("")
    val cacheControl = get("cacheControl").getOrElse("")
    val outputFileExtension = get("outputFileExtension").getOrElse("")

    println(tsName)
    println(ifWithMirco)


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