package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.creditkarma.blink.utils.gcs.GCSUtils
import com.creditkarma.blink.utils.writer.CkAutoTsMessageParser
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
/**
  * Created by shengwei.wang on 12/7/16.
  */
class GCSWriter(
                      tsName: String,
                      ifWithMicro: Boolean,
                      enforcedFields: String,
                      credentialsPath: String,
                      connectTimeoutMs: Int,
                      readTimeoutMs: Int,
                      bucketName:String,
                      outputAppString:String,
                      metaData:String,
                      cacheControl:String,
                      outputFileExtension:String
                    ) extends ExportWorker[String,String,GCSSubPartition]{
  override def useSubPartition: Boolean = true

  // messages with unparseable ts all go to the default partition
  private val defaulPartitionYear:String = "1969"
  private val defaulPartitionMonth:String = "12"
  private val defaulPartitionDay:String = "31"

  override def getSubPartition(payload: String): GCSSubPartition = {
    val mTsParser = new CkAutoTsMessageParser(tsName,ifWithMicro,enforcedFields)
    val result = Try(
    {
      mTsParser.extractTimestampMillis(payload, "")
    }    )
            match {
              case Success(x) => new GCSSubPartition(x.year,x.month,x.day,x.hour,x.minute,x.second)
              case Failure(f) => new GCSSubPartition(defaulPartitionYear,defaulPartitionMonth,defaulPartitionDay,"","","")
              }
    result
  }

  override def write(partition: SubPartition[GCSSubPartition],
                     data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {

    def topic = partition.topic
    def topicPartitn = partition.partition
    def subPartition = partition.subPartition
    def firstOffset = partition.fromOffset
    def fileName = s"${topic}/${topicPartitn}/${subPartition.map(_.getYear).getOrElse(defaulPartitionYear)}/${subPartition.map(_.getMonth).getOrElse(defaulPartitionMonth)}/${subPartition.map(_.getDay).getOrElse(defaulPartitionDay)}/${firstOffset}.${outputFileExtension}"

    var lines = 0L
    var bytes = 0L
    // construct a stream for gcs uploader
    val mInputStreamContent = new InputStreamContent(
      outputAppString, // example "application/json",
      iteratorToStream(
        data.map {
          record: KafkaMessageWithId[String, String] => {
            lines += 1
            bytes += record.value.size
            record.value + "\n"
          }
        }
      )
    )

    val metaDataMap: util.HashMap[String,String] = new util.HashMap[String,String]()
    for(keyString <- metaData.split(";")){
      metaDataMap.put(keyString.split(",")(0),keyString.split(",")(1))
    }

    val result=
    Try (
      {val request =
        GCSUtils
          .getService(// get gcs storage service
            credentialsPath,
            connectTimeoutMs, // connection timeout
            readTimeoutMs // read timeout
          )
          .objects.insert(// insert object
          bucketName, // gcs bucket
          new StorageObject().setMetadata(metaDataMap).setCacheControl(cacheControl).setName(fileName), // gcs object name
          mInputStreamContent
        )
      request.getMediaHttpUploader.setDirectUploadEnabled(true)
      request.execute()
  }
      )
    match {
      case Success(_) => new WorkerMeta(lines,bytes,true)
      case Failure(f) => new WorkerMeta(lines,bytes,false)
    }
    result
  }

  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => {new ByteArrayInputStream(s.getBytes("UTF-8"))} }
      i.asJavaEnumeration
    })
  }

}
