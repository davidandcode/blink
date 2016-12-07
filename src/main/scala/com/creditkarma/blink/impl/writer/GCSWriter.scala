package com.creditkarma.blink.impl.writer

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.creditkarma.blink.impl.transformer.KafkaMessageWithId
import com.creditkarma.blink.utils.gcs.GCSUtils
import com.creditkarma.blink.utils.writer.CkAutoTsMessageParser
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable.{StreamIterator, VectorIterator}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
                      credentials:String,
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
    val tsParseResult = mTsParser.parseAsTsString(payload)

    new GCSSubPartition(tsParseResult.year,tsParseResult.month,tsParseResult.day,tsParseResult.hour,tsParseResult.minute,tsParseResult.second)
  }

  /**
    * For a [[KafkaPartitionWriter]], it's write status is atomic (all or nothing):
    * either the entire partition is successfully written or none.
    * Even if the writer client supports partial writes, it's still treated atomically by the framework.
    *
    * @param topicPartition the full path should have other necessary prefix such as the gcs bucket etc.
    * @param subPartition   sub-partition within the kafka topicPartition, such as time based partition
    * @param data           stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    *
    *         all unparsed messages go to a default partition
    */
  override def write(topicPartition: TopicPartition, firstOffset: Long, subPartition: Option[GCSSubPartition], data: Iterator[KafkaMessageWithId[String, String]]): WriterClientMeta = {
    // need to create 2 requests: one for all parsable lines; the other for unparsable lines
    val mParser: CkAutoTsMessageParser = new CkAutoTsMessageParser(tsName, ifWithMicro, enforcedFields)

    val good: mutable.MutableList[KafkaMessageWithId[String, String]] = 	new mutable.MutableList[KafkaMessageWithId[String, String]]()
    val bad: mutable.MutableList[KafkaMessageWithId[String, String]] = 	new mutable.MutableList[KafkaMessageWithId[String, String]]()

    var goodLines:Long = 0
    var goodBytes:Long = 0
    var flag:Boolean = true

    for(temp <- data){

      val eachM = new Array[String](1)
      eachM(0) = temp.value
      try{
        mParser.extractTimestampMillis(eachM,topicPartition.topic())
        goodLines +=1
        goodBytes += eachM(0).getBytes.length
        good+=(new KafkaMessageWithId[String,String](temp.key,eachM(0),temp.kmId,temp.batchFirstOffset))
      } catch{
        case e:Exception => {bad += temp}
      }

    }


    val metaData:util.HashMap[String,String] = new util.HashMap[String,String]()
    metaData.put("priority",null)
    metaData.put("period","60")


    try {


    val requestBad =
      GCSUtils
        .getService( // get gcs storage service
          credentialsPath,
          connectTimeoutMs, // connection timeout
          readTimeoutMs // read timeout
        )
        .objects.insert( // insert object
        bucketName, // gcs bucket
        new StorageObject().setCacheControl(cacheControl).setMetadata(metaData).setName(s"${topicPartition.topic()}/${topicPartition.partition()}/${subPartition.get.getYear}/${subPartition.get.getMonth}/${subPartition.get.getDay}/${firstOffset}.json"), // gcs object name
        new InputStreamContent(
          outputAppString, // example "application/json",
          iteratorToStream(
            bad.toIterator.map{
              record: KafkaMessageWithId[String, String] => record.value + "\n"
            }
          )
        )
      )
    requestBad.getMediaHttpUploader.setDirectUploadEnabled(true)
    requestBad.execute()

    val requestGood =
        GCSUtils
          .getService(// get gcs storage service
            credentialsPath,
            connectTimeoutMs, // connection timeout
            readTimeoutMs // read timeout
          )
          .objects.insert(// insert object
          bucketName, // gcs bucket
          new StorageObject().setCacheControl(cacheControl).setMetadata(metaData).setName(s"${topicPartition.topic()}/${topicPartition.partition()}/${subPartition.get.getYear}/${subPartition.get.getMonth}/${subPartition.get.getDay}/${firstOffset}.json"), // gcs object name
          new InputStreamContent(
            outputAppString, // example "application/json",
            iteratorToStream(
              good.toIterator.map {
                record: KafkaMessageWithId[String, String] => record.value + "\n"
              }
            )
          )
        )
      requestGood.getMediaHttpUploader.setDirectUploadEnabled(true)
      requestGood.execute()

      new WriterClientMeta(goodLines,goodBytes,true)

    } catch {
      case e:Exception => { new WriterClientMeta(goodLines,goodBytes,false)}
    }


  }


  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => {new ByteArrayInputStream(s.getBytes("UTF-8"))} }
      i.asJavaEnumeration
    })
  }

}
