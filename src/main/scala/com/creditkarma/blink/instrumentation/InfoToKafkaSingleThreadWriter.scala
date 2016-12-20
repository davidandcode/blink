package com.creditkarma.blink.instrumentation

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by shengwei.wang on 12/8/16.
  * This writer is designed to run in a single thread and it is NOT for general purpurse but only for instrumentation.
  */
class InfoToKafkaSingleThreadWriter(host:String,port:String,topicName:String,seesionTO:String) {
  val keyString = "INSTRUMENTATION"
  val  props = new Properties()
  props.put("bootstrap.servers",host + ":" + port) //props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("session.timeout.ms", seesionTO);

  val producer = new KafkaProducer[String, String](props)

  def saveBlockToKafka(blockData:Seq[String]): Unit ={
    for(eachLine <- blockData){
      val record = new ProducerRecord(topicName, keyString, eachLine)
      producer.send(record)
    }
  }

}
