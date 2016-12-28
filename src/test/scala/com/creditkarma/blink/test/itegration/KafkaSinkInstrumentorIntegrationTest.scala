package com.creditkarma.blink.test.itegration

import com.creditkarma.blink.base.{OperationMode, StateTracker, TimeMode}
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import com.creditkarma.blink.instrumentation.{KafkaSinkInstrumentor, LogInfoInstrumentor}
import net.minidev.json.{JSONObject, JSONValue}

/**
  * Created by shengwei.wang on 12/8/16.
  */
class KafkaSinkInstrumentorIntegrationTest extends KafkaIntegrationTestBase {
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: StateTracker[KafkaCheckpoint] = new InMemoryKafkaStateTracker

  feature("Very simple basic test") {

    scenario("Metrics string should be parseable json strings with correct fields") {


      Given("1 topic created")
      createTopic("test1", 1)

      val nMessage = 3000

      for(i <- 1 to nMessage){
        sendMessage("test1", "testingkey", s"message${i}")
      }


      val portalId = "test-portal"
      val portal = getOrCreatePortal(portalId, flushSize = 100)

      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      val mInstrumentor = new KafkaSinkInstrumentor(60,"localhost",s"${brokerPort}","metrics","100000")
      portal.registerInstrumentor(mInstrumentor)
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

      assert(allMessages != null,"nothing flushed")

      for(temp <-allMessages){
        if(temp.topicPartition.topic().charAt(0) != '_' && temp.topicPartition.topic() == "metrics"){
          And("that is from Kafka: " + "topic = " + temp.topicPartition.topic() + " partition = " + temp.topicPartition.partition() + " and the message " + temp.value + " " + temp.key)
         val jsonObject = JSONValue.parse(temp.value ).asInstanceOf[JSONObject]
          assert(JSONValue.parse(jsonObject.getAsString("payload")).asInstanceOf[JSONObject].getAsNumber("nRecordsErr") == 0, "Blink should have no failure")
          assert(JSONValue.parse(jsonObject.getAsString("payload")).asInstanceOf[JSONObject].getAsString("RecordsErrReason") == "there are no failures in this minute!" , "Blink should have no failure")
          assert(JSONValue.parse(jsonObject.getAsString("payload")).asInstanceOf[JSONObject].getAsString("RecordsErrReason") == "there are no failures in this minute!" , "Blink should have no failure")
          assert( jsonObject.getAsString("eventType") == "TableTopics_g0_t01_p0_DPMetrics" )
          assert( jsonObject.getAsString("dataSource") == "Blink_Out" )
          assert( jsonObject.getAsString("userAgent") == portalId )

        }
      }
    }
  }
}