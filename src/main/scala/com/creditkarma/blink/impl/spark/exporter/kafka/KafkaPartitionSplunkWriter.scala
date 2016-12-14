package com.creditkarma.blink.impl.spark.exporter.kafka

/**
  * Created by shengwei.wang on 12/10/16.
  */
class KafkaPartitionSplunkWriter extends ExportWorker[String, String, String]{
  override def useSubPartition: Boolean = true

  override def getSubPartition(payload: String): String = ???

  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = ???
}
