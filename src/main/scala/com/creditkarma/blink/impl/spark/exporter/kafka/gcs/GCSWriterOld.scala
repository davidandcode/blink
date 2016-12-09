package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
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
class GCSWriterOld(
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

    val mParser: CkAutoTsMessageParserOld = new CkAutoTsMessageParserOld(tsName, ifWithMicro, enforcedFields)
    val dataAfter: mutable.MutableList[KafkaMessageWithId[String, String]] = 	new mutable.MutableList[KafkaMessageWithId[String, String]]()

    var lines:Long = 0
    var bytes:Long = 0
    var flag:Boolean = true

    for(temp <- data){
        lines +=1
        bytes += temp.value.getBytes.length
    }

    // if bad data, no parsing/correction at all and all goes to the defaul folder
    if(subPartition.get.getYear == defaulPartitionYear && subPartition.get.getMonth == defaulPartitionMonth && subPartition.get.getDay == defaulPartitionDay){

    try {
      val requestBad =
        GCSUtils
          .getService(// get gcs storage service
            credentialsPath,
            connectTimeoutMs, // connection timeout
            readTimeoutMs // read timeout
          )
          .objects.insert(// insert object
          bucketName, // gcs bucket
          new StorageObject().setCacheControl(cacheControl).setName(s"${topicPartition.topic()}/${topicPartition.partition}/${defaulPartitionYear}/${defaulPartitionMonth}/${defaulPartitionDay}/${firstOffset}.json"), // gcs object name
          new InputStreamContent(
            outputAppString, // example "application/json",
            iteratorToStream(
              data.map {
                record: KafkaMessageWithId[String, String] => record.value + "\n"
              }
            )
          )
        )
      requestBad.getMediaHttpUploader.setDirectUploadEnabled(true)
      requestBad.execute()
      return new WorkerMeta(lines,bytes,true)
    } catch {
      case e:Exception => {return new WorkerMeta(lines,bytes,false)}
      }
    } else { // if good data, still needs parsing / correction on non standard time stamp

      for(temp <- data){
        val eachM = new Array[String](1)
        eachM(0) = temp.value
        try{
          mParser.extractTimestampMillis(eachM,topicPartition.topic())
          dataAfter+=(new KafkaMessageWithId[String,String](temp.key,eachM(0),temp.kmId))
        } catch{
          case e:Exception => {} // it is guaranteed if it is good data, everything in it is parseable so we never come to here but if we come here, swallow the exception/ skipping this message
        }
      }

      val metaData:util.HashMap[String,String] = new util.HashMap[String,String]()
      metaData.put("priority",null)
      metaData.put("period","60")

      try{
        metaData.put("rows", lines.toString)
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
                dataAfter.toIterator.map {
                  record: KafkaMessageWithId[String, String] => record.value + "\n"
                }
              )
            )
          )
        requestGood.getMediaHttpUploader.setDirectUploadEnabled(true)
        requestGood.execute()

        return  new WorkerMeta(lines,bytes,true)
      } catch {
        case e:Exception => { return new WorkerMeta(lines,bytes,false)}
      }



    }

  }


  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => {new ByteArrayInputStream(s.getBytes("UTF-8"))} }
      i.asJavaEnumeration
    })
  }

}
