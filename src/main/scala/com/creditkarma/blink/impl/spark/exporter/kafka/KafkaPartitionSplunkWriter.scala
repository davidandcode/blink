package com.creditkarma.blink.impl.spark.exporter.kafka

/**
  * Created by shengwei.wang on 12/10/16.
  */
class KafkaPartitionSplunkWriter(index:String) extends ExportWorker[String, String, String]{

  override def useSubPartition: Boolean = false

  override def getSubPartition(payload: String): String = ???

  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = ???
}
