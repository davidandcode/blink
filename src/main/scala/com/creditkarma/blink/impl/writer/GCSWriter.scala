package com.creditkarma.blink.impl.writer

import com.creditkarma.blink.impl.transformer.KafkaMessageWithId
import com.creditkarma.blink.utils.writer.CkAutoTsMessageParser
import org.apache.kafka.common.TopicPartition

/**
  * Created by shengwei.wang on 12/7/16.
  */
class GCSWriter[K,V](
                      tsName: String,
                      ifWithMicro: Boolean,
                      enforcedFields: String

                    ) extends KafkaPartitionWriter[K, V, GCSSubPartition]{
  override def useSubPartition: Boolean = true



  override def getSubPartition(payload: V): GCSSubPartition = {
    val payloadString:String = payload.toString

    val mTsParser = new CkAutoTsMessageParser(tsName,ifWithMicro,enforcedFields)
    val tsParseResult = mTsParser.parseAsTsString(payloadString)
   // val year = tsParseResult.date.toString.substring()

    new GCSSubPartition("","","","","","","","")
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
    */
  override def write(topicPartition: TopicPartition, firstOffset: Long, subPartition: Option[GCSSubPartition], data: Iterator[KafkaMessageWithId[K, V]]): WriterClientMeta = ???
}
