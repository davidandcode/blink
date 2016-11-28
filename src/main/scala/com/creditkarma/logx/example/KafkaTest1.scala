package com.creditkarma.logx.example

import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.Utils
import com.creditkarma.logx.base.{BufferedData, CheckpointService, Transformer, Writer, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.impl.transformer.{KafkaMessageWithId, KafkaSparkMessageIdTransformer}
import com.creditkarma.logx.impl.writer._
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

  class CollectorMeta(records: Long, bytes: Long) extends WriterMeta[Seq[OffsetRange]]{
    override def metrics: Metrics =
      new Metrics {
        override def metrics: Iterable[Metric] =
          Seq(new Metric {
            override def dimensions: Map[Any, Any] = Map.empty
            override def fields: Map[Any, Any] = Map("records"->records, "bytes"->bytes)
          })
      }

    override def outRecords: Long = records
  }
  class KafkaSparkRDDMessageCollector(collectedData: ListBuffer[String])
    extends Writer[SparkRDD[KafkaMessageWithId[String, String]], KafkaCheckpoint, Seq[OffsetRange], CollectorMeta]{
    override def write(data: SparkRDD[KafkaMessageWithId[String, String]], lastCheckpoint: KafkaCheckpoint,
                       inTime: Long, inDelta: Seq[OffsetRange]): CollectorMeta = {
      val inData = data.rdd.map(_.value).collect()
      collectedData ++= inData
      new CollectorMeta(inData.size, inData.map(_.size).sum)
    }
  }

  val partitioner = new MessagePartitioner[String, String] {
    override def contentBasedPartition(payload: String): Option[String] = None
  }

  val outputWriterCreator = new KafkaMessageOutputClientCreator[String, String]{
    override def createClient(): KafkaMessageOutputClient[String, String] = {
      new KafkaMessageOutputClient[String, String] {
        //val collectedData: ListBuffer[String] = ListBuffer.empty
        override def write(partition: String, data: Iterable[KafkaMessageWithId[String, String]]): WriterClientMeta = {
          val messages = data.map(_.value).toSeq
          //collectedData ++= messages
          WriterClientMeta(bytes=messages.map(_.size).sum, records = messages.size, complete=true)
        }
      }
    }

  }
  val collectorWriter = new KafkaSparkRDDPartitionedWriter[String, String](partitioner, outputWriterCreator)



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
    val testLogX = Utils.createKafkaSparkFlow(
      "test-logX", kafkaParams, new KafkaSparkRDDMessageCollector(collectedData), new InMemoryKafkaCheckpointService())


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

    //TestKafkaServer.sendNextMessage(21)
    //testLogX.start()

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
