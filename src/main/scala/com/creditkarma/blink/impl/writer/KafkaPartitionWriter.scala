package com.creditkarma.blink.impl.writer

import com.creditkarma.blink.client.{ClientModuleType, SparkWorkerModule}
import com.creditkarma.blink.impl.transformer.KafkaMessageWithId
import org.apache.kafka.common.TopicPartition

/**
  * Created by yongjia.wang on 12/7/16.
  */
trait KafkaPartitionWriter[K, V, P] extends SparkWorkerModule {
  def useSubPartition: Boolean
  def getSubPartition(payload: V): P
  /**
    * For a [[KafkaPartitionWriter]], it's write status is atomic (all or nothing):
    * either the entire partition is successfully written or none.
    * Even if the writer client supports partial writes, it's still treated atomically by the framework.
    *
    * @param partition sub-partition within the kafka topicPartition, such as time based partition
    * @param data stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    */
  def write(partition: KafkaSubPartition[P], data: Iterator[KafkaMessageWithId[K, V]]): WriterClientMeta

  override final def moduleType: ClientModuleType.Value = ClientModuleType.SingleThreadWriter
}
