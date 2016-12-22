package com.creditkarma.blink.test.itegration

import com.creditkarma.blink.base.{OperationMode, StateTracker, TimeMode}
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import com.creditkarma.blink.instrumentation.{KafkaSinkInstrumentor, LogInfoInstrumentor}

/**
  * Created by shengwei.wang on 12/8/16.
  */
class KafkaSinkInstrumentorIntegrationTest extends KafkaIntegrationTestBase {
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: StateTracker[KafkaCheckpoint] = new InMemoryKafkaStateTracker

  feature("Very simple basic test") {

    scenario("Kafka should receive all messages") {
      
      /**
        * The setup of test here is very confusing
        * If kafka writer is not general purpose, it should be an inner class of the instrumentor, and take exactly the same arguments from the instrumentor
        *
        */

      /*val mWriter = new InfoToKafkaSingleThreadWriter("localhost",s"${brokerPort}","SOME_TEST_TOPIC","100000")

      val mSeq = new mutable.MutableList[String]

      for(i <- 1 to 100000){
        mSeq += s"message:${i}"
      }

      mWriter.saveBlockToKafka(mSeq)
      */

      val portalId = "test-portal"
      val portal = getOrCreatePortal(portalId, flushSize = 100)

      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      val mInstrumentor = new KafkaSinkInstrumentor(6000,"localhost",s"${brokerPort}","metrics","100000")
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