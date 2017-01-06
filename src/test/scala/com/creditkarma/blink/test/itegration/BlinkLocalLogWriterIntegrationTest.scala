package com.creditkarma.blink.test.itegration

/**
  * Created by shengwei.wang on 12/21/16.
  */

import java.io.{File, PrintWriter}

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.test.LocalSpark
import org.scalatest._

import scala.io.Source

class BlinkLocalLogWriterIntegrationTest extends FlatSpec with LocalKafka[String, String] with LocalSpark  {


  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))


  "A writer" should "upload the data/message correctly" in {


    startKafka()

    sendMessage("LocalTest", "instrumentation", "This is the 1st message.")
    sendMessage("LocalTest", "instrumentation", "This is the 2nd message.")
    sendMessage("LocalTest", "instrumentation", "This is the 3rd message.")


    //assert(Source.fromFile("src/test/resources/generated_local_output/test_file_integration").getLines().length == 6)

    assert(Source.fromFile("src/test/resources/generated_local_output/test_file_integration").getLines().forall(line => line.startsWith("This is")))


    val dir = "src/test/resources/generated_config"
    val fileName = s"$dir/kafka_local_log.properties"
    new File(dir).mkdirs()
    new PrintWriter(fileName) { write(configWithLocalPrefix); close }

    val portalId = MainApp.castPortal(fileName)

    shutDownKafka()
  }

  private def configWithLocalPrefix = {
    s"""
       |#===================================================================================================
       |# Blink portal configurations
       |#===================================================================================================
       |blink.portal.factory.class=com.creditkarma.blink.factory.KafkaImportPortalFactoryStringType
       |blink.portal.factory.properties.id=kafka-test2
       |blink.portal.factory.properties.kafka.bootstrap.servers=localhost:1234
       |blink.portal.factory.properties.kafka.whitelist="LocalTest"
       |blink.portal.factory.properties.writer.creator.class=com.creditkarma.blink.impl.spark.exporter.kafka.local.LocalLogWriterCreator
       |#this is just some custom property going to the injected class
       |blink.portal.factory.properties.writer.creator.properties.p1=v1
       |blink.portal.factory.properties.zookeeper.host=localhost:5678
       |blink.portal.factory.properties.flush.interval=1000
       |blink.portal.factory.properties.flush.size=1
       |
 |#===================================================================================================
       |# Client configurations -- Local Log Writer
       |#===================================================================================================
       |blink.portal.factory.properties.writer.creator.properties.localFileName=src/test/resources/generated_local_outputsrc/test/resources/generated_local_output/test_log_file_integration
       |blink.portal.factory.properties.writer.creator.properties.maxFileSize=10MB
       |blink.portal.factory.properties.writer.creator.properties.MaxBackupIndex=2
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