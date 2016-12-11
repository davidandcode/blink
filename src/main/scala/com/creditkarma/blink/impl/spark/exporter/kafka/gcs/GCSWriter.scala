package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util
import java.util.Date
import java.text.SimpleDateFormat;

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
  private val defaultEpochTime = 0L
  val format:SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
  val defaultPartitionPath:String = format.format(new Date(defaultEpochTime))

  override def getSubPartition(payload: String): GCSSubPartition = {
    val mTsParser = new CkAutoTsMessageParser(tsName,ifWithMicro,enforcedFields)
    val result = Try(
    {
      mTsParser.extractTimestampMillis(payload, "")
    }    )
            match {
              case Success(x) => new GCSSubPartition(x.year,x.month,x.day)
              case Failure(f) => new GCSSubPartition(defaultPartitionPath)
              }
    result
  }

  override def write(partition: SubPartition[GCSSubPartition], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {
    def subPartition = partition.subPartition
    def fileName =
      s"""${partition.topic}/${partition.partition}/
         |${subPartition.map(_.timePartitionPath).getOrElse(defaultPartitionPath)}/
         |${partition.fromOffset}.${outputFileExtension}""".stripMargin.replaceAll("\n", "")

    var lines = 0L
    var bytes = 0L

    val mStream = iteratorToStream(
      data.map {
        record: KafkaMessageWithId[String, String] => {
          lines += 1
          bytes += record.value.size
          record.value + "\n"
        }
      }
    )

    val metaDataMap: util.HashMap[String,String] = new util.HashMap[String,String]()
    for(keyString <- metaData.split(";")){
      metaDataMap.put(keyString.split(",")(0),keyString.split(",")(1))
    }

    val result = Try (
      {
        val storageObject = GCSUtils.getService(credentialsPath,connectTimeoutMs,readTimeoutMs).objects()
        val request = storageObject.insert(
          bucketName,
          new StorageObject().setMetadata(metaDataMap).setCacheControl(cacheControl).setName(fileName),
          new InputStreamContent( outputAppString, mStream)
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
