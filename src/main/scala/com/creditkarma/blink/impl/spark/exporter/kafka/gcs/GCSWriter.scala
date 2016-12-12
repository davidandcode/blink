package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.creditkarma.blink.utils.gcs.GCSUtils
import com.creditkarma.blink.utils.writer.CkAutoTsMessageParser
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject
import org.apache.commons.lang.time.DateUtils

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
                      outputFileExtension:String,
                      pathPrefix:String
                    ) extends ExportWorker[String,String,GCSSubPartition]{
  override def useSubPartition: Boolean = true

  override def getSubPartition(payload: String): GCSSubPartition = {
    val mTsParser = new CkAutoTsMessageParser(tsName,ifWithMicro,enforcedFields)
    val result = Try(
    {
      val timeLong = mTsParser.extractTimestampMillis(payload, "")
      DateUtils.truncate(new Date(timeLong),Calendar.DATE).getTime

    }    )
            match {
              case Success(x) => new GCSSubPartition(x)
              case Failure(f) => new GCSSubPartition(0)
    }

    result
  }

  override def write(partition: SubPartition[GCSSubPartition], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {
    def subPartition = partition.subPartition
    def fileName =
      s"""${pathPrefix}${partition.topic}/
         |${subPartition.map(_.timePartitionPath).getOrElse("")}/
         |${partition.partition}/${partition.fromOffset}.${outputFileExtension}""".stripMargin.replaceAll("\n", "")

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
