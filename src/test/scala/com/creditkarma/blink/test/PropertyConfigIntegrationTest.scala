package com.creditkarma.blink.test

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.factory.KafkaStringPartitionWriterCreator
import com.creditkarma.blink.impl.transformer.KafkaMessageWithId
import com.creditkarma.blink.impl.writer.{KafkaPartitionWriter, WriterClientMeta}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, LogManager}
import org.scalatest.{BeforeAndAfterAll, WordSpec}

/**
  * This integration test starts from a single configuration file to test end-to-end
  * In most other cases, integration test should start from any convenient entry points for most flexibility
  */
class PropertyConfigIntegrationTest extends WordSpec with BeforeAndAfterAll with KafkaIntegrationTest[String, String]{

  // port numbers must match the configuration file
  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))
  val configFile = "src/test/resources/kafka.test.properties"

  "A blink portal lunched by valid config" should {
    "receive all 10 messages" in {
      prepareKafkaData()
      val portalId = MainApp.castPortal(configFile)
      assert(SimpleCollectibleWriter.globalCollector.get(portalId).get.size == 10)
      shutDownKafka()
    }
  }

  def prepareKafkaData(): Unit = {
    startKafka()
    LogManager.getLogger("org.apache").setLevel(Level.WARN)
    LogManager.getLogger("kafka").setLevel(Level.WARN)

    createTopic("test1", 2)
    createTopic("test2", 3)

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
  }
}

class TestWriterCreator extends KafkaStringPartitionWriterCreator {
  override def writer: KafkaPartitionWriter[String, String, String] = {
    SimpleCollectibleWriter.writer
  }
}