package com.creditkarma.blink.test

/**
  * Created by shengwei.wang on 12/21/16.
  */

import java.io.{File, PrintWriter}
import com.creditkarma.blink.MainApp
import org.scalatest._

class BlinkLocalWriterIntegrationTest extends FlatSpec with KafkaIntegrationTest[String, String] with SparkLocalMaster  {


  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))


  "A writer" should "upload the data/message correctly" in {


    startKafka()

    sendMessage("LocalTest", "instrumentation", "This is the 1st message.")
    sendMessage("LocalTest", "instrumentation", "This is the 2nd message.")
    sendMessage("LocalTest", "instrumentation", "This is the 3rd message.")


    val dir = "src/test/resources/generated_config"
    val fileName = s"$dir/kafka_local.properties"
    new File(dir).mkdirs()
    new PrintWriter(fileName) { write(configWithSplunkPrefix); close }

    val portalId = MainApp.castPortal(fileName)

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
       |blink.portal.factory.properties.kafka.whitelist="LocalTest"
       |blink.portal.factory.properties.writer.creator.class=com.creditkarma.blink.impl.spark.exporter.kafka.local.LocalWriterCreator
       |#this is just some custom property going to the injected class
       |blink.portal.factory.properties.writer.creator.properties.p1=v1
       |blink.portal.factory.properties.zookeeper.host=localhost:5678
       |blink.portal.factory.properties.flush.interval=1000
       |blink.portal.factory.properties.flush.size=1
       |
 |#===================================================================================================
       |# Client configurations -- Local Writer
       |#===================================================================================================
       |blink.portal.factory.properties.writer.creator.properties.localPath=src/test/resources/generated_local_output
       |blink.portal.factory.properties.writer.creator.properties.localFileName=test_file_integration
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