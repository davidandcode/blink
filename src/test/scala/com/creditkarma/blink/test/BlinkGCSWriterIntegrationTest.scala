package com.creditkarma.blink.test

import com.creditkarma.blink.factory.KafkaPartitionGCSWriterCreator
import com.creditkarma.blink.impl.spark.exporter.kafka.ExportWorker
import com.creditkarma.blink.impl.spark.exporter.kafka.gcs.{GCSSubPartition, GCSWriter}

/**
  * Created by shengwei.wang on 12/9/16.
  */
class BlinkGCSWriterIntegrationTest {

}





class GCSTestWriterCreator extends KafkaPartitionGCSWriterCreator {
  override def writer: ExportWorker[String, String, GCSSubPartition] = {

    val tsName = get("tsName")
    val ifWithMirco = get("ifWithMicro").toBoolean
    val enforcedFields = get("enforcedFields")
    val credentialsPath = get("credentialsPath")
    val connectTimeoutMs = get("connectTimeoutMs").toInt
    val readTimeoutMs = get("readTimeoutMs").toInt
    val bucketName = get("bucketName")
    val outputAppString = get("outputAppString")
    val metaData = get("metaData")
    val cacheControl = get("cacheControl")
    val outputFileExtension = get("outputFileExtension")
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