package com.creditkarma.blink.test

import com.creditkarma.blink.impl.transformer.{KafkaMessageId, KafkaMessageWithId}
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
/**
  * Created by yongjia.wang on 11/29/16.
  */
trait KafkaIntegrationTest[K, V]{
  val configuredPorts: Option[(Int, Int)] = None
  private lazy val kafkaUnitServer = configuredPorts match {
    case Some((zPort, kPort)) => new KafkaUnit(zPort, kPort)
    case None => new KafkaUnit()
  }
  private var cachedProducer: Option[KafkaProducer[K, V]] = None
  def getOrCreateProducer: KafkaProducer[K, V] = {
    cachedProducer.getOrElse{
      cachedProducer = Some(new KafkaProducer[K, V](Map[String, Object](
        "bootstrap.servers" -> s"localhost:${brokerPort}",
        "key.serializer" -> classOf[StringSerializer],
        "value.serializer" -> classOf[StringSerializer]
      ).asJava))
      cachedProducer.get
    }
  }

  private var cachedConsumer: Option[KafkaConsumer[K, V]] = None
  def getOrCreateConsumer: KafkaConsumer[K, V] = {
    cachedConsumer.getOrElse{
      cachedConsumer = Some(new KafkaConsumer[K, V](Map[String, Object](
        "bootstrap.servers" -> s"localhost:${brokerPort}",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test"
      ).asJava))
      cachedConsumer.get
    }
  }

  def zkPort: Int = kafkaUnitServer.getZkPort
  def brokerPort: Int = kafkaUnitServer.getBrokerPort

  def sendMessage(topic: String, key: K, value: V): Unit = {
    getOrCreateProducer.send(new ProducerRecord[K, V](topic, key, value)).get()
  }

  def createTopic(topic: String, numPartitions: Int): Unit = {
    kafkaUnitServer.createTopic(topic, numPartitions)
  }

  var _allMessages: Seq[KafkaMessageWithId[K, V]] = Seq.empty


  /**
    *
    * @return message key value pair augmented with topic partition and offset
    */
  def allMessages: Seq[KafkaMessageWithId[K, V]] = {
    if(_allMessages.isEmpty){
      val consumer = getOrCreateConsumer
      val topics = consumer.listTopics().asScala.flatMap(_._2.asScala)
        .map {
          pi => new TopicPartition(pi.topic(), pi.partition())
        }.toSeq
      consumer.assign(topics.asJava)
      _allMessages =
      topics.flatMap{
        tp =>
          consumer.seekToBeginning(Seq(tp).asJava)
          val fromOffset = consumer.position(tp)
          val timeout = 1000
          val records = consumer.poll(timeout)
          records.records(tp).asScala.zipWithIndex.map{
            case(record, index) =>
              KafkaMessageWithId(record.key(), record.value(), KafkaMessageId(tp, fromOffset + index), fromOffset)
          }
      }
    }
    _allMessages

  }

  def startKafka(): Unit = {
    kafkaUnitServer.startup()
  }

  def shutDownKafka(): Unit = {
    cachedProducer.map(_.close())
    cachedConsumer.map(_.close())
    kafkaUnitServer.shutdown()
  }
}
