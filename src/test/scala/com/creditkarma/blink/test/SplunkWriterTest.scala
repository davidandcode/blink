package com.creditkarma.blink.test

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util

import com.creditkarma.blink.impl.spark.exporter.kafka._
import com.creditkarma.blink.impl.spark.exporter.kafka.splunk.KafkaPartitionSplunkWriter
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
class SplunkWriterTest extends FlatSpec{


  val mWriter = new KafkaPartitionSplunkWriter("test_index",Array("localhost"),"admin","changemeyes", 8089)


  "A writer" should "upload the data/message correctly" in {

    val loginArgs: ServiceArgs = new ServiceArgs
    loginArgs.setUsername("admin")
    loginArgs.setPassword("changemeyes")
    loginArgs.setHost("localhost")
    loginArgs.setPort(8089)
    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2)
    val service: Service = Service.connect(loginArgs)
    val myIndex: Index = service.getIndexes.get("test_index")
    myIndex.clean(180)


    // use the Blink's splunk writer to write into Splunk
    val testTopicPartition: TopicPartition = new TopicPartition("SplunkWriterUnitTest", 5)
    val fakeKMI = KafkaMessageId(testTopicPartition,1)
    val data: scala.collection.mutable.MutableList[KafkaMessageWithId[String, String]] = new scala.collection.mutable.MutableList[KafkaMessageWithId[String, String]]
    val payload1 = "This is the 1st message for Blink's splunk writer test."
    val payload2 = "This is the 2nd message for Blink's splunk writer test."
    val payload3 = "This is the 3rd message for Blink's splunk writer test."
    data += new KafkaMessageWithId[String,String]("testKey", payload1, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", payload2, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", payload3, fakeKMI)

    val subPartition = SubPartition(OffsetRange(testTopicPartition, 1234567, 2345678), Some(mWriter.getSubPartition(payload1)))
    val result: WorkerMeta = mWriter.write(subPartition, data.toIterator)

    assert(result.complete == true, "Write job failure.")
    println(result.records == 3, "not all records are written into Splunk")

    // Wait for the writer to finish writting
    Thread.sleep(10000)

    //read from splunk use splunk client app
      val mySearch: String = "search index=test_index"
      val jobargs:JobArgs  = new JobArgs()
      jobargs.setExecutionMode(JobArgs.ExecutionMode.NORMAL)
      val job:Job  = service.getJobs().create(mySearch, jobargs)

      // Wait for the search to finish
      while (!job.isDone()) {
          Thread.sleep(500)
      }


      val resultsNormalSearch:InputStream  =  job.getResults()
      var resultsReaderNormalSearch:ResultsReaderXml = null


        resultsReaderNormalSearch = new ResultsReaderXml(resultsNormalSearch)
        var event:util.HashMap[String, String] = resultsReaderNormalSearch.getNextEvent()

        while (event != null) {

          System.out.println("\n****************EVENT****************\n")
          for (key <- event.keySet())
            println("   " + key + ":  " + event.get(key))

          event = resultsReaderNormalSearch.getNextEvent()

        }

    assert(job.getEventCount() == 3,"Not all events are read from splunk")


      // Get properties of the completed job
      println("\nSearch job properties\n---------------------")
      println("Search job ID:         " + job.getSid())
      println("The number of events:  " + job.getEventCount())
      println("The number of results: " + job.getResultCount())
      println("Search duration:       " + job.getRunDuration() + " seconds")
      println("This job expires in:   " + job.getTtl() + " seconds")

    myIndex.clean(180)

  }


}
