package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.impl.spark.{ClientModuleType, SparkWorkerModule}

/**
  * Created by yongjia.wang on 12/7/16.
  */
trait ExportWorker[K, V, P] extends SparkWorkerModule {
  def useSubPartition: Boolean
  def getSubPartition(payload: V): P
  /**
    * For a [[ExportWorker]], it's write status is atomic (all or nothing):
    * either the entire partition is successfully written or none.
    * Even if the writer client supports partial writes, it's still treated atomically by the framework.
    *
    * @param partition sub-partition within the kafka topicPartition, such as time based partition
    * @param data stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    */
  def write(partition: SubPartition[P], data: Iterator[KafkaMessageWithId[K, V]]): WorkerMeta

  override final def moduleType: ClientModuleType.Value = ClientModuleType.SingleThreadWriter
}

case class WorkerMeta(records: Long, bytes: Long, complete: Boolean, message: String = "")
