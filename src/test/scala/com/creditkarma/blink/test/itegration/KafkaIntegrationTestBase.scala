package com.creditkarma.blink.test.itegration

import com.creditkarma.blink.PortalConstructor
import com.creditkarma.blink.base.{Portal, StateTracker}
import com.creditkarma.blink.impl.spark.buffer.SparkRDD
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import com.creditkarma.blink.test.LocalSpark
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, GivenWhenThen}

/**
  * Created by yongjia.wang on 12/22/16.
  */
trait KafkaIntegrationTestBase extends FeatureSpec with BeforeAndAfterAll with GivenWhenThen with LocalKafka[String, String] with LocalSpark {
  type Key = String
  type Value = String
  type Partition = String
  type PortalType = Portal[SparkRDD[ConsumerRecord[Key, Value]], SparkRDD[ConsumerRecord[Key, Value]], KafkaCheckpoint, Seq[OffsetRange]]
  type WriterType = CollectibleTestWriter[Key, Value, Partition]

  def getWriter: WriterType
  def getCheckpointService: StateTracker[KafkaCheckpoint]

  private val _kafkaPortals = collection.mutable.Map.empty[String, PortalType]

  val defaultFlushInterval = 1000
  def getOrCreatePortal(portalId: String, flushSize: Long, flushInterval: Long = defaultFlushInterval): PortalType = {
    _kafkaPortals.getOrElseUpdate(
      portalId,
      PortalConstructor.createKafkaSparkPortalWithSingleThreadedWriter(
        portalId, Map[String, Object](
          "bootstrap.servers" -> s"localhost:${brokerPort}",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "test"
        ), getWriter.writer, getCheckpointService, flushInterval, flushSize)
    )
  }

  override def beforeAll(): Unit = {
    startKafka()
    startLocalSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    shutDownKafka()
  }
}
