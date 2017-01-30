package com.creditkarma.blink.test.integration

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}

import scala.collection.mutable.ListBuffer

/**
  * This writer asserts the input stream (iterator) to the write function is as expected
  * 1. Offset must be in increasing order. This is only implicitly guaranteed by Spark Kafka RDD's mapPartition and groupBy operations,
  * therefore it's important to verify in test.
  * 2. All messages must have the same topic-partition and optional sub-partition, this is explicitly guaranteed by Spark API.
  * Each writer runs as a serialized task, the global collector only works in local mode.
  * In cluster mode, the writers will be in different JVMs.
  */
object SimpleCollectibleWriter extends CollectibleTestWriter[String, String, String]{
  val globalCollector: collection.mutable.Map[String, ListBuffer[KafkaMessageWithId[String, String]]] = collection.mutable.Map.empty
  /**
    * This is the writer being tested
    *
    * @return
    */
  override def writer: ExportWorker[String, String, String] = new ExportWorker[String, String, String] {

    override def useSubPartition: Boolean = false

    // won't be called if useSubPartition is false
    override def getSubPartition(payload: String): String = ""

    override def write(partition: SubPartition[String],
                       data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {
      // make sure message offset is in order here
      def firstOffset = partition.fromOffset
      def topicPartition = partition.topicPartition
      def subPartition = partition.subPartition
      var records = 0
      var bytes = 0
      var previousOffset: Option[Long] = None
      while(data.hasNext){
        val message = data.next()
        records += 1
        bytes += message.value.size
        addMessageToClobalCollector(message)
        assert(previousOffset.isEmpty || previousOffset.get < message.kmId.offset) // this is not neccesarily true after groupBy
        assert(firstOffset <= message.kmId.offset)
        assert(message.kmId.topicPartition == topicPartition)
        subPartition match {
          case Some(sp) =>
            assert(getSubPartition(message.value) == sp)
          case None =>
        }
        previousOffset = Option(message.kmId.offset)
      }
      WorkerMeta(records, bytes, true)
    }

    private def addMessageToClobalCollector(message: KafkaMessageWithId[String, String]) =
      globalCollector.synchronized{
        globalCollector.getOrElseUpdate(portalId, ListBuffer.empty[KafkaMessageWithId[String, String]]) += message
      }

  }

  /**
    * This must be implemented based on knowledge of writer to collect information back
    *
    * @return
    */
  override def collect: Map[String, Seq[KafkaMessageWithId[String, String]]] = globalCollector.toMap

  override def clearAll(): Unit = globalCollector.clear()
}
