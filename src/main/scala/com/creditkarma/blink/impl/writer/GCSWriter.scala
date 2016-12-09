package com.creditkarma.blink.impl.writer

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.creditkarma.blink.impl.transformer.KafkaMessageWithId
import com.creditkarma.blink.utils.gcs.GCSUtils
import com.creditkarma.blink.utils.writer.{CkAutoTsMessageParser, CkAutoTsMessageParserOld}
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable


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

                    ) extends KafkaPartitionWriter[String,String,GCSSubPartition]{
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

  override def write(topicPartition: TopicPartition, firstOffset: Long, subPartition: Option[GCSSubPartition], data: Iterator[KafkaMessageWithId[String, String]]): WriterClientMeta = {

    var lines = 0L
    var bytes = 0L

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
          new StorageObject().setMetadata(metaData).setCacheControl(cacheControl).setName(s"${topicPartition.topic()}/${topicPartition.partition}/${subPartition.get.getYear}/${subPartition.get.getMonth}/${subPartition.get.getDay}/${firstOffset}.json"), // gcs object name
          mInputStreamContent
        )
      request.getMediaHttpUploader.setDirectUploadEnabled(true)
      request.execute()
      return new WriterClientMeta(lines,bytes,true)
    } catch {
      case e:Exception => {return new WriterClientMeta(lines,bytes,false)}
      }
  }

  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => {new ByteArrayInputStream(s.getBytes("UTF-8"))} }
      i.asJavaEnumeration
    })
  }

}
