package com.creditkarma.logx.example

import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.Utils
import com.creditkarma.logx.base.CheckpointService
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.impl.transformer.KafkaMessageWithId
import com.creditkarma.logx.impl.writer.{KafkaPartitionedWriter, KafkaSparkRDDPartitionedWriter, WriterClientMeta}
import com.creditkarma.logx.instrumentation.LogInfoInstrumentor
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Created by yongjia.wang on 11/28/16.
  */
object KafkaPartitionedOutputTest {
  val testWriter = new KafkaPartitionedWriter[String, String, String]{
    override def useSubPartition: Boolean = true
    // won't be called if useSubPartition is false
    override def getSubPartition(payload: String): String = ""
    override def write(topicPartition: TopicPartition, subPartition: Option[String], data: Iterable[KafkaMessageWithId[String, String]]): WriterClientMeta = {
      // make sure message offset is in order here
      val itr = data.iterator
      var currentMessage = itr.next()
      while(itr.hasNext){
        val nextMessage = itr.next()
        assert(currentMessage.kmId.offset < nextMessage.kmId.offset)
        assert(currentMessage.kmId.topicPartition == nextMessage.kmId.topicPartition)
        assert(getSubPartition(currentMessage.value) == getSubPartition(nextMessage.value))
        currentMessage = nextMessage
      }
      WriterClientMeta(data.size, data.map(_.value.size).sum, true)
    }
  }
  val collectorWriter = new KafkaSparkRDDPartitionedWriter[String, String, String](testWriter)


  class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
    var lastCheckPoint: KafkaCheckpoint = new KafkaCheckpoint()
    override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
      lastCheckPoint = cp
    }
    override def lastCheckpoint(): KafkaCheckpoint = {
      lastCheckPoint
    }
  }

  def main(args: Array[String]): Unit = {
    TestKafkaServer.start()
    TestKafkaServer.loadData("test_data/KRSOffer.json")

    LogManager.getLogger("org.apache").setLevel(Level.WARN)
    LogManager.getLogger("kafka").setLevel(Level.WARN)


    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local[2]")
      .set("spark.driver.host", "127.0.0.1")
      // set local host explicitly, the call through java.net.InetAddress.getLocalHost on laptop with VPN can be inconsistent
      // Also if it returns IPV6, Spark won't work with it
    )

    val testLogX = Utils.createKafkaSparkFlow(
      "test-logX", kafkaParams, collectorWriter, new InMemoryKafkaCheckpointService(), flushInterval = 2000, flushSize = 2)


    testLogX.registerInstrumentor(LogInfoInstrumentor)

    TestKafkaServer.sendNextMessage(10)
    testLogX.runOneCycle()
    //println(collectedData)
    TestKafkaServer.sendNextMessage(2)
    testLogX.runOneCycle()
    //println(collectedData)
    TestKafkaServer.sendNextMessage(10)
    testLogX.runOneCycle()
    //println(collectedData)

    //TestKafkaServer.sendNextMessage(21)
    //testLogX.start()

    println(TestKafkaServer.sentMessages.size)

    testLogX.close()
    sc.stop()
    Thread.sleep(1000)
    TestKafkaServer.stop()

    println("Demo succeeded")

  }


  val (zkPort, kafkaPort) = (5556, 5558)

  object TestKafkaServer {

    val kafkaUnitServer = new KafkaUnit(zkPort, kafkaPort)
    val kp = new KafkaProducer[String, String](kafkaParams.asJava)

    def start(): Unit = {
      kafkaUnitServer.startup()
      kafkaUnitServer.createTopic("testTopic", 5)
    }

    val testData: ListBuffer[String] = ListBuffer.empty
    var dataSentToKafka = 0

    def loadData (dataFile: String): Unit = {
      val br = new BufferedReader(new FileReader(dataFile))
      while({
        val line = br.readLine()
        if(line != null){
          testData.append(line)
          true
        }
        else{
          false
        }
      }){
      }
    }

    def sendNextMessage(n: Int): Unit = {
      var i = 0
      while(i < n && sendNextMessage()){
        i += 1
      }
    }

    def sendNextMessage(): Boolean = {
      if(dataSentToKafka == testData.size){
        false
      }
      else {
        kp.send(new ProducerRecord[String, String]("testTopic", null, testData(dataSentToKafka)))
        dataSentToKafka += 1
        true
      }
    }

    def sentMessages: Seq[String] = testData.slice(0, dataSentToKafka)

    def stop(): Unit = {
      kp.close()
      kafkaUnitServer.shutdown()
    }
  }

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> s"localhost:$kafkaPort",
    "key.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.serializer" -> classOf[StringSerializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test"
  )

}
