package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import com.creditkarma.blink.factory.KafkaExportWorkerCreator

import scala.util.{Failure, Success, Try}

class GCSWriterCreator extends KafkaExportWorkerCreator[String, String, String] {
  override def writer: GCSWriter = {

    val tsName = getOrFail("tsName")
    val partitionFormat = get("partitionFormat").getOrElse("yyyy/MM/dd")
    val ifWithMirco = getOrFail("ifWithMicro").toBoolean
    val enforcedFields = getOrFail("enforcedFields")
    val credentialsPath = get("credentialsPath").getOrElse("")
    val connectTimeoutMs = get("connectTimeoutMs").getOrElse("10000").toInt
    val readTimeoutMs = get("readTimeoutMs").getOrElse("10000").toInt
    val bucketName = get("bucketName").getOrElse("")
    val outputAppString = get("outputAppString").getOrElse("")
    val metaData = get("metaData").getOrElse("")
    val cacheControl = get("cacheControl").getOrElse("")
    val outputFileExtension = get("outputFileExtension").getOrElse("")
    val pathPrefix = get("pathPrefix").getOrElse("")
    val compression = getOrFail("compression")

    // validation must be done in the main thread before the writer objects are created and later serialized to each task
    Try(new TimePartitioner(partitionFormat)) match {
      case Success(_) =>
      case Failure(f) => throw new Exception(s"partitionFormat=$partitionFormat is invalid", f)
    }

    new GCSWriter(
      tsName,
      partitionFormat,
      ifWithMirco,
      enforcedFields,
      credentialsPath,
      connectTimeoutMs,
      readTimeoutMs,
      bucketName,
      outputAppString,
      metaData,
      cacheControl,
      outputFileExtension,
      pathPrefix,
      compression
    )
  }
}
