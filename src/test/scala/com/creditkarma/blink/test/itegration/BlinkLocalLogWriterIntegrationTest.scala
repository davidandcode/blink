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
  val log_file_path = "src/test/resources/generated_local_output/test_log_file_integration"
  new File(log_file_path).delete()

  "A writer" should "upload the data/message correctly" in {


    startKafka()

    val messages = Seq("This is the 1st message.", "This is the 2nd message.", "This is the 3rd message.")
    for (message <- messages) {
      sendMessage("LocalTest", "instrumentation", message)
    }

    val dir = "src/test/resources/generated_config"
    val fileName = s"$dir/kafka_local_log.properties"
    new File(dir).mkdirs()
    new PrintWriter(fileName) { write(configWithLocalPrefix); close }



    val portalId = MainApp.castPortal(fileName)
    assert(Source.fromFile(log_file_path ).getLines().toSeq == messages)

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
       |blink.portal.factory.properties.writer.creator.properties.localFileName=${log_file_path }
       |blink.portal.factory.properties.writer.creator.properties.maxFileSize=50KB
       |blink.portal.factory.properties.writer.creator.properties.MaxBackupIndex=1
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