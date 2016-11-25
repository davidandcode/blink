package com.creditkarma.logx.example

import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.base.{BufferedData, CheckpointService, Transformer, Writer, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.instrumentation.LogInfoInstrumentor
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by yongjia.wang on 11/17/16.
  */

object KafkaTest1 {

  class IdentityTransformer[I <: BufferedData] extends Transformer[I, I]{
    override def transform(input: I): I = input
  }


  class KafkaSparkRDDMessageCollector(collectedData: ListBuffer[String])
    extends Writer[SparkRDD[ConsumerRecord[String, String]], Seq[OffsetRange], (Long, Long)]{

    override def write(data: SparkRDD[ConsumerRecord[String, String]]): (Long, Long) = {
      val inData = data.rdd.map(_.value()).collect()
      collectedData ++= inData
      (inData.size, inData.map(_.size).sum)
    }
    override def getMetrics(meta: (Long, Long)): Seq[Map[Any, Any]] = {
      Seq(Map("records"->meta._1, "bytes"->meta._2))
    }
  }

  class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
    var lastCheckPoint: KafkaCheckpoint = new KafkaCheckpoint()
    override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
      lastCheckPoint = cp
    }

    override def lastCheckpoint(): KafkaCheckpoint = {
      lastCheckPoint
    }
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



    val collectedData: ListBuffer[String] = ListBuffer.empty
    val reader = new KafkaSparkRDDReader[String, String](kafkaParams)
    reader.setMaxFetchRecordsPerPartition(1)
    val testLogX = new LogXCore(
      "test-logX",
      reader = reader,
      transformer = new IdentityTransformer[SparkRDD[ConsumerRecord[String, String]]](),
      writer = new KafkaSparkRDDMessageCollector(collectedData),
      checkpointService = new InMemoryKafkaCheckpointService()
    )


    testLogX.registerInstrumentor(LogInfoInstrumentor)

    TestKafkaServer.sendNextMessage(6)
    testLogX.runOneCycle()
    //println(collectedData)
    TestKafkaServer.sendNextMessage(4)
    testLogX.runOneCycle()
    //println(collectedData)
    TestKafkaServer.sendNextMessage(4)
    testLogX.runOneCycle()
    //println(collectedData)

    println(collectedData.size)
    println(TestKafkaServer.sentMessages.size)
    assert(collectedData.sorted == TestKafkaServer.sentMessages.sorted)

    testLogX.close()
    sc.stop()
    Thread.sleep(1000)
    TestKafkaServer.stop()

    println("Demo succeeded")
  }

  def loadDataToLocalKafka(kafkaUnitServer: KafkaUnit) = {
    kafkaUnitServer.startup()

    val kp = new KafkaProducer[String, String](kafkaParams.asJava)
    val br = new BufferedReader(new FileReader("test_data/KRSOffer.json"))
    while({
      val line = br.readLine()
      kp.send(new ProducerRecord[String, String]("testTopic", null, line))
      kp.flush()
      line != null
    }){
    }

    kp.close()
  }
}
