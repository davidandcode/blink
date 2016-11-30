package com.creditkarma.logx.test
import com.creditkarma.logx.Utils
import com.creditkarma.logx.base.{CheckpointService, LogXCore}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.transformer.KafkaMessageWithId
import com.creditkarma.logx.impl.writer.KafkaSparkRDDPartitionedWriter
import com.creditkarma.logx.instrumentation.LogInfoInstrumentor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
/**
  * Kafka Integration Test trait with pluggable modules
  * This version only support plugging checkpoint service and the single threaded writer, which are the main focus of testing right now.
  * In the future, a more flexible testing trait can be used to plugin very thing including source data test data generator.
  */
trait BlinkKafkaIntegrationTest extends FeatureSpec with BeforeAndAfterAll with GivenWhenThen with KafkaIntegrationTest[String, String] with SparkLocalMaster {
  type Key = String
  type Value = String
  type Partition = String
  type PortalType = LogXCore[SparkRDD[ConsumerRecord[Key, Value]], SparkRDD[KafkaMessageWithId[Key, Value]], KafkaCheckpoint, Seq[OffsetRange]]
  type WriterType = CollectibleTestWriter[Key, Value, Partition]

  def getWriter: WriterType
  def getCheckpointService: CheckpointService[KafkaCheckpoint]

  private val _kafkaPortals = collection.mutable.Map.empty[String, PortalType]

  val defaultFlushInterval = 1000
  def getOrCreatePortal(portalId: String, flushSize: Long, flushInterval: Long = defaultFlushInterval): PortalType = {
    _kafkaPortals.getOrElseUpdate(portalId, Utils.createKafkaSparkPortalWithSingleThreadedWriter(
      portalId, kafkaParams, getWriter.writer, getCheckpointService, flushInterval, flushSize))
  }

  override def beforeAll(): Unit = {
    startKafka()
    startLocalSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    for((portalId, portal) <- _kafkaPortals){
      portal.close()
    }
    shutDownKafka()
  }


  feature("Very simple basic test") {

    scenario("Kafka should receive all messages") {

      Given("2 topics created")
      createTopic("test1", 2)
      createTopic("test2", 3)

      And("5 messages sent to each topic")
      sendMessage("test1", "k1", "v1")
      sendMessage("test1", "k2", "v2")
      sendMessage("test1", "k3", "v3")
      sendMessage("test1", "k4", "v4")
      sendMessage("test1", "k5", "v5")

      sendMessage("test2", "k1", "v1")
      sendMessage("test2", "k2", "v2")
      sendMessage("test2", "k3", "v3")
      sendMessage("test2", "k4", "v4")
      sendMessage("test2", "k5", "v5")

      Then("there should be 10 messages in total")
      assert(allMessages.size == 10)
    }

    scenario("Send all data to a single portal with one flush") {

      Given("a test-portal which will flush all messages in one cycle")
      val portalId = "test-portal"
      val portal = getOrCreatePortal(portalId, flushSize = 50)

      And("the log instrumentor is registered to show the trace, but otherwise doesn't matter")
      portal.registerInstrumentor(LogInfoInstrumentor) // this is just to observe trace

      When("running the portal until no data is pushed")
      portal.runUntilNoPush()

      Then("the writer threads should collectively observe the same data")
      assert(getWriter.collect.get(portalId).get.sortBy(_.toString) == allMessages.sortBy(_.toString))
    }

    scenario("Two portals from Kafka with independent checkpoint") {
      Given("2 portals each flush 1 message per time per partition")
      val id1 = "portal_1of2"
      val id2 = "portal_2of2"
      val portal1 = getOrCreatePortal(id1, flushSize = 1)
      val portal2 = getOrCreatePortal(id2, flushSize = 1)

      And("the log instrumentor is registered to show the trace, but otherwise doesn't matter")
      portal1.registerInstrumentor(LogInfoInstrumentor) // this is just to observe trace
      portal2.registerInstrumentor(LogInfoInstrumentor) // this is just to observe trace

      portal1.runUntilNoPush()
      portal2.runUntilNoPush()

      Then("the writer threads should collectively observe the same data for each portal")
      assert(getWriter.collect.get(id1).get.sortBy(_.toString) == allMessages.sortBy(_.toString))
      assert(getWriter.collect.get(id2).get.sortBy(_.toString) == allMessages.sortBy(_.toString))
    }
  }
}