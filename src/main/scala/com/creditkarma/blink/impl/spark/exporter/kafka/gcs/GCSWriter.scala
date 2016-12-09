package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.creditkarma.blink.utils.gcs.GCSUtils
import com.creditkarma.blink.utils.writer.CkAutoTsMessageParser
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject

import scala.collection.JavaConverters._


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
                      cacheControl:String

                    ) extends ExportWorker[String,String,GCSSubPartition]{
  override def useSubPartition: Boolean = true

  private val defaulPartitionYear:String = "1969"
  private val defaulPartitionMonth:String = "12"
  private val defaulPartitionDay:String = "31"

  override def getSubPartition(payload: String): GCSSubPartition = {
    val mTsParser = new CkAutoTsMessageParser(tsName,ifWithMicro,enforcedFields)

try {
  val tsParseResult = mTsParser.extractTimestampMillis(payload, "")
   return new GCSSubPartition(tsParseResult.year,tsParseResult.month,tsParseResult.day,tsParseResult.hour,tsParseResult.minute,tsParseResult.second)
} catch { // any parsing failure leads to default path/partition
  case e:Exception => return new GCSSubPartition(defaulPartitionYear,defaulPartitionMonth,defaulPartitionDay,"","","")
}
  }

  override def write(partition: SubPartition[GCSSubPartition],
                     data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {

    def topic = partition.topic
    def topicPartition = partition.topicPartition
    def subPartition = partition.subPartition
    def firstOffset = partition.fromOffset

    var lines = 0L
    var bytes = 0L

    val itr = new Iterator[KafkaMessageWithId[String, String]] {
      override def hasNext: Boolean = data.hasNext

      override def next(): KafkaMessageWithId[String, String] = {
        lines += 1
        val record = data.next()
        bytes += record.value.size
        record
      }
    }


    val mInputStreamContent = new InputStreamContent(
      outputAppString, // example "application/json",
      iteratorToStream(
        data.map {
          record: KafkaMessageWithId[String, String] => {
            lines += 1L
            bytes += record.value.getBytes().length
            record.value + "\n"
          }
        }
      )
    )

    val metaData: util.HashMap[String,String] = new util.HashMap[String,String]()
    metaData.put("period","60")
    metaData.put("priority",null)
    metaData.put("rows",lines.toString)

    try {
      val request =
        GCSUtils
          .getService(// get gcs storage service
            credentialsPath,
            connectTimeoutMs, // connection timeout
            readTimeoutMs // read timeout
          )
          .objects.insert(// insert object
          bucketName, // gcs bucket
          new StorageObject().setMetadata(metaData).setCacheControl(cacheControl).setName(s"${topicPartition.topic()}/${topicPartition.partition}/${subPartition.map(_.getYear).getOrElse("")}/${subPartition.get.getMonth}/${subPartition.get.getDay}/${firstOffset}.json"), // gcs object name
          mInputStreamContent
        )
      request.getMediaHttpUploader.setDirectUploadEnabled(true)
      request.execute()
      return new WorkerMeta(lines,bytes,true)
    } catch {
      case e:Exception => {return new WorkerMeta(lines,bytes,false)}
      }
  }

  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => {new ByteArrayInputStream(s.getBytes("UTF-8"))} }
      i.asJavaEnumeration
    })
  }

}
