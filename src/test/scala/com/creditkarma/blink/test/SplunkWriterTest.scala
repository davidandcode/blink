package com.creditkarma.blink.test

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.creditkarma.blink.impl.spark.exporter.kafka._
import com.splunk._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest.FlatSpec

/**
  * Created by shengwei.wang on 12/20/16.
  */
class SplunkWriterTest extends FlatSpec{

  // Create a map of arguments and add login parameters
  val loginArgs: ServiceArgs = new ServiceArgs
  loginArgs.setUsername("admin")
  loginArgs.setPassword("changemeyes")
  loginArgs.setHost("localhost")
  loginArgs.setPort(8089)
  // Create a Service instance and log in with the argument map


  val mWriter = new KafkaPartitionSplunkWriter("test_index",Array("localhost"),"admin","changemeyes", 8089)


  "A writer" should "upload the data/message correctly" in {

    val testTopicPartition: TopicPartition = new TopicPartition("SplunkWriterUnitTest", 5)
    val fakeKMI = KafkaMessageId(testTopicPartition,1)
    val data: scala.collection.mutable.MutableList[KafkaMessageWithId[String, String]] = new scala.collection.mutable.MutableList[KafkaMessageWithId[String, String]]
    val payload1 = "This is the first message"
    val payload2 = "This is the 2nd message"
    data += new KafkaMessageWithId[String,String]("testKey", payload1, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", payload2, fakeKMI)
    val subPartition = SubPartition(OffsetRange(testTopicPartition, 1234567, 2345678), Some(mWriter.getSubPartition(payload1)))
    val result: WorkerMeta = mWriter.write(subPartition, data.toIterator)


    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2)
    val service: Service = Service.connect(loginArgs)
    val myIndex: Index = service.getIndexes.get("test_index")

    val mySearch:String = "search index=test_index";
    val job:Job  = service.getJobs().create(mySearch);

    // Wait for the job to finish
    while (!job.isDone()) {
      Thread.sleep(500);
    }

    // Display results
    val results:InputStream  = job.getResults();
    val br:BufferedReader = new BufferedReader(new InputStreamReader(results, "UTF-8"));
    var line:String = br.readLine()

    while (line != null) {
      println(line)
      line = br.readLine()
    }

    br.close();
    myIndex.clean(180)

  }


}
