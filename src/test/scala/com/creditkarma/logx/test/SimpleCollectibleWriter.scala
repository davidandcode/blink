package com.creditkarma.logx.test
import com.creditkarma.logx.client.ClientModuleType
import com.creditkarma.logx.impl.transformer.KafkaMessageWithId
import com.creditkarma.logx.impl.writer.{KafkaPartitionWriter, WriterClientMeta}
import org.apache.kafka.common.TopicPartition

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
  override def writer: KafkaPartitionWriter[String, String, String] = new KafkaPartitionWriter[String, String, String] {

    override def useSubPartition: Boolean = false

    // won't be called if useSubPartition is false
    override def getSubPartition(payload: String): String = ""

    override def write(topicPartition: TopicPartition, subPartition: Option[String], data: Iterator[KafkaMessageWithId[String, String]]): WriterClientMeta = {
      // make sure message offset is in order here
      val itr = data
      var currentMessage = itr.next()
      var records = 1
      var bytes = currentMessage.value.size
      addMessageToClobalCollector(currentMessage)

      while(itr.hasNext){
        val nextMessage = itr.next()
        records += 1
        bytes += nextMessage.value.size
        addMessageToClobalCollector(nextMessage)
        assert(currentMessage.kmId.offset < nextMessage.kmId.offset)
        assert(currentMessage.kmId.topicPartition == nextMessage.kmId.topicPartition)
        assert(getSubPartition(currentMessage.value) == getSubPartition(nextMessage.value))
        currentMessage = nextMessage
      }
      WriterClientMeta(records, bytes, true)
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
