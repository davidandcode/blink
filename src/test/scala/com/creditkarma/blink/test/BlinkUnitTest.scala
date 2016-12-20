package com.creditkarma.blink.test

/**
  * Created by shengwei.wang on 12/14/16.
  */
import com.creditkarma.blink.PortalConstructor
import com.creditkarma.blink.base.{OperationMode, Portal, StateTracker, TimeMode}
import com.creditkarma.blink.impl.spark.buffer.SparkRDD
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import com.creditkarma.blink.instrumentation.{InfoToKafkaInstrumentor, InfoToKafkaSingleThreadWriter, LogInfoInstrumentor}
import net.minidev.json.{JSONObject, JSONValue}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest._

import scala.collection.mutable
/**
  * Kafka Integration Test trait with pluggable modules
  * This version only support plugging checkpoint service and the single threaded writer, which are the main focus of testing right now.
  * In the future, a more flexible testing trait can be used to plugin very thing including source data test data generator.
  */
trait BlinkUnitTest extends FeatureSpec with BeforeAndAfterAll with GivenWhenThen with KafkaIntegrationTest[String, String] with SparkLocalMaster {
  type Key = String
  type Value = String
  type Partition = String
  type PortalType = Portal[SparkRDD[ConsumerRecord[Key, Value]], SparkRDD[ConsumerRecord[Key, Value]], KafkaCheckpoint, Seq[OffsetRange]]
  type WriterType = CollectibleTestWriter[Key, Value, Partition]

  def getWriter: WriterType
  def getCheckpointService: StateTracker[KafkaCheckpoint]

  private val _kafkaPortals = collection.mutable.Map.empty[String, PortalType]

  val defaultFlushInterval = 100
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


  feature("Very simple basic test") {

    scenario("Kafka should receive all messages") {



      val mWriter = new InfoToKafkaSingleThreadWriter("localhost",s"${brokerPort}","SOME_TEST_TOPIC","100000")

     val mSeq = new mutable.MutableList[String]

      for(i <- 1 to 100000){
      mSeq += s"message:${i}"
      }

      mWriter.saveBlockToKafka(mSeq)

      val portalId = "test-portal"
      val portal = getOrCreatePortal(portalId, flushSize = 100)

      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      val mInstrumentor = new InfoToKafkaInstrumentor(6000,"localhost",s"${brokerPort}","metrics","100000")
      portal.registerInstrumentor(mInstrumentor)
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

      assert(allMessages != null,"nothing flushed")

      for(temp <-allMessages){
        if(temp.topicPartition.topic().charAt(0) != '_' && temp.topicPartition.topic() == "metrics")
          And("that is from Kafka: " + "topic = " + temp.topicPartition.topic() + " partition = " + temp.topicPartition.partition() + " and the message " + temp.value + " " + temp.key)

      }


    }

  }



}