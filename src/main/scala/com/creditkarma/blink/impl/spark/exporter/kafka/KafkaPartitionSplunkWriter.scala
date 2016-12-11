package com.creditkarma.blink.impl.spark.exporter.kafka

/**
  * Created by shengwei.wang on 12/10/16.
  */
class KafkaPartitionSplunkWriter extends KafkaPartitionWriter[String, String, MetricsSubpartition]{
  override def useSubPartition: Boolean = ???

  override def getSubPartition(payload: String): MetricsSubpartition = ???

  /**
    * For a [[KafkaPartitionWriter]], it's write status is atomic (all or nothing):
    * either the entire partition is successfully written or none.
    * Even if the writer client supports partial writes, it's still treated atomically by the framework.
    *
    * @param partition sub-partition within the kafka topicPartition, such as time based partition
    * @param data      stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    */
  override def write(partition: KafkaSubPartition[MetricsSubpartition], data: Iterator[KafkaMessageWithId[String, String]]): WriterClientMeta = ???
}
