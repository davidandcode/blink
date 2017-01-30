package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.impl.spark.{ClientModuleType, SparkWorkerModule}

/**
  * Created by yongjia.wang on 12/7/16.
  */
trait ExportWorkerWithSubPartition[K, V, P] extends SparkWorkerModule {
  def useSubPartition: Boolean

  /**
    * It's very important this method should never throw out exception
    * @param payload
    * @return
    */
  def getSubPartition(payload: V): P
  /**
    * It's very important this method should never throw out exception.
    * Instead, it can try to recover from exceptions such as network issues,
    * perform some optional retries, and return the proper flag and messages in the [[WorkerMeta]].
    * It should also handle any payload anomaly such as null payload, and make progress with the writing task.
    * The only time it is allowed not to make progress with writing is due to things like network, which is out of control,
    * and assumed to be auto-recoverable.
    * If the writer implementation is not able to make progress due to some deterministic edge cases, the in/out metrics and
    * error messages are propagated back to the main thread and sent to metrics services, so the problem can be exposed and fixed.
    * For a [[ExportWorkerWithSubPartition]], it's write status is atomic (all or nothing):
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

trait ExportWorker[K, V] extends ExportWorkerWithSubPartition[K, V, AnyRef] {
  override def useSubPartition: Boolean = false
  def getSubPartition(payload: V): AnyRef = None
}
