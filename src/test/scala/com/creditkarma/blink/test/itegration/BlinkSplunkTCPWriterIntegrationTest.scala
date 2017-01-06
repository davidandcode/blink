package com.creditkarma.blink.test.itegration

import java.io.{File, InputStream, PrintWriter}
import java.util

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.test.LocalSpark
import com.splunk._
import org.scalatest._

import scala.collection.JavaConversions._

/**
  * Created by shengwei.wang on 12/9/16.
  * This test requires a splunk instance up and running
  */
class BlinkSplunkTCPWriterIntegrationTest extends FlatSpec with LocalKafka[String, String] with LocalSpark  {


  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))


  "A writer" should "upload the data/message correctly" in {

    val loginArgs: ServiceArgs = new ServiceArgs
    loginArgs.setUsername("admin")
    loginArgs.setPassword("changemeyes")
    loginArgs.setHost("localhost")
    loginArgs.setPort(8089)
    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2)
    val service: Service = Service.connect(loginArgs)
    val myIndex: Index = service.getIndexes.get("test_index")
    // clean up splunk instance
    myIndex.clean(180)

    startKafka()

    sendMessage("SplunkTest", "instrumentation", "This is the 1st message.")
    sendMessage("SplunkTest", "instrumentation", "This is the 2nd message.")
    sendMessage("SplunkTest", "instrumentation", "This is the 3rd message.")


    val dir = "src/test/resources/generated_config"
    val fileName = s"$dir/kafka_splunk_tcp.properties"
    new File(dir).mkdirs()
    new PrintWriter(fileName) { write(configWithSplunkPrefix); close }

    val portalId = MainApp.castPortal(fileName)

    Thread.sleep(10000)

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

   // myIndex.clean(180)




    shutDownKafka()
  }

  private def configWithSplunkPrefix = {
s"""
   |#===================================================================================================
   |# Blink portal configurations
   |#===================================================================================================
   |blink.portal.factory.class=com.creditkarma.blink.factory.KafkaImportPortalFactoryStringType
   |blink.portal.factory.properties.id=kafka-test2
   |blink.portal.factory.properties.kafka.bootstrap.servers=localhost:1234
   |blink.portal.factory.properties.kafka.whitelist="SplunkTest"
   |blink.portal.factory.properties.writer.creator.class=com.creditkarma.blink.impl.spark.exporter.kafka.splunk.SplunkTCPWriterCreator
   |#this is just some custom property going to the injected class
   |blink.portal.factory.properties.writer.creator.properties.p1=v1
   |blink.portal.factory.properties.zookeeper.host=localhost:5678
   |blink.portal.factory.properties.flush.interval=1000
   |blink.portal.factory.properties.flush.size=1
   |
 |#===================================================================================================
   |# Client configurations -- Splunk TCP Writer
   |#===================================================================================================
   |blink.portal.factory.properties.writer.creator.properties.indexers=127.0.0.1:2468
   |
 |#===================================================================================================
   |# Blink engine configurations
   |#===================================================================================================
   |# Spark is the only engine now, but there could be more
   |blink.engine=spark
   |
 |#===================================================================================================
   |# Third party configurations
   |# 3rd party properties such as SparkConf properties, are as is
   |# Blink's configuration system will detect and relay them
   |#===================================================================================================
   |spark.master=local[*]
   |spark.app.name=blink-kafka-test
   |spark.executor.memory=1G
   |spark.driver.host=127.0.0.1
 """.stripMargin
  }
}
