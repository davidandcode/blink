package com.creditkarma.blink.test

import java.io.InputStream
import java.util

import com.creditkarma.blink.impl.spark.exporter.kafka._
import com.creditkarma.blink.impl.spark.exporter.kafka.splunk.{KafkaPartitionSplunkTCPWriter, KafkaPartitionSplunkWriter}
import com.splunk._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest.FlatSpec

import scala.collection.JavaConversions._

/**
  * Created by shengwei.wang on 12/20/16.
  * This test requires having a splunk instance running on the same host. There is no splunk instance mocking utility.
  *
  */
class SplunkTCPWriterTest extends FlatSpec{


  val mWriter = new KafkaPartitionSplunkTCPWriter(Array(("localhost","1234")))


  "A writer" should "upload the data/message correctly" in {

    // use the Blink's splunk writer to write into Splunk
    val testTopicPartition: TopicPartition = new TopicPartition("SplunkWriterUnitTest", 5)
    val fakeKMI = KafkaMessageId(testTopicPartition,1)
    val data: scala.collection.mutable.MutableList[KafkaMessageWithId[String, String]] = new scala.collection.mutable.MutableList[KafkaMessageWithId[String, String]]
    val payload1 = "This is the 1st message for Blink's splunk log writer test."
    val payload2 = "This is the 2nd message for Blink's splunk log writer test."
    val payload3 = "This is the 3rd message for Blink's splunk log writer test."
    data += new KafkaMessageWithId[String,String]("testKey", payload1, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", payload2, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", payload3, fakeKMI)

    val subPartition = SubPartition(OffsetRange(testTopicPartition, 1234567, 2345678), Some(mWriter.getSubPartition(payload1)))
    val result: WorkerMeta = mWriter.write(subPartition, data.toIterator)

    assert(result.complete == true, "Write job failure.")
    println(result.records == 3, "not all records are written into Splunk")



  }


}
