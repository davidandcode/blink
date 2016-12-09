package com.creditkarma.blink.test
import com.creditkarma.blink.PortalConstructor
import com.creditkarma.blink.base.{StateTracker, OperationMode, Portal, TimeMode}
import com.creditkarma.blink.impl.spark.buffer.SparkRDD
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import com.creditkarma.blink.instrumentation.LogInfoInstrumentor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange
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
      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace

      When("running the portal until no data is pushed")
      //portal.runTilCompletion()
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

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
      portal1.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      portal2.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace

      portal1.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)
      portal2.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

      Then("the writer threads should collectively observe the same data for each portal")
      assert(getWriter.collect.get(id1).get.map(_.toString).sorted == allMessages.map(_.toString).sorted)
      assert(getWriter.collect.get(id2).get.map(_.toString).sorted  == allMessages.map(_.toString).sorted )
    }

    scenario("Portal must be able to reset position") {
      Given("a new portal")
      val id = "new-portal"
      val portal = getOrCreatePortal(id, flushSize = 1)
      And("the log instrumentor is registered to show the trace, but otherwise doesn't matter")
      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      When("starting from now")
      And("running the portal until completion")
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Now)
      Then("nothing should have been written")
      assert(getWriter.collect.getOrElse(id, Seq.empty).isEmpty)

      When("starting from earliest")
      And("running the portal until completion")
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)
      Then("the writer threads should collectively observe the same data")
      assert(getWriter.collect.get(id).get.map(_.toString).sorted == allMessages.map(_.toString).sorted)
    }
  }
}