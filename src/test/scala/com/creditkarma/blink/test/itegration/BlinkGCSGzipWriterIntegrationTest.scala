package com.creditkarma.blink.test.itegration

import java.io.{File, PrintWriter}

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.test.{GCSTest, LocalSpark}
import org.scalatest._

/**
  * Created by shengwei.wang on 12/9/16.
  */
class BlinkGCSGzipWriterIntegrationTest extends FlatSpec with LocalKafka[String, String] with LocalSpark with GCSTest {

  sc.hadoopConfiguration.set("fs.gs.project.id", "295779567055")
  sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.email", "dataeng-test@modular-shard-92519.iam.gserviceaccount.com")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", "testingcredentials/DataScience-ac040bae47fb.p12")
  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))
  override val testType: String = "integration"

  "A writer" should "upload the data/message correctly" in {
    startKafka()
    //parseables
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2014-05-07T14:22:35.863513-08:00\"}")
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-11-27T14:22:35.863513-08:00\"}")
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2019-05-07T14:22:35.863513-08:00\"}")
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863513-08:00\"}")
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863000-08:00\"}")
    //unparseables
    sendMessage("SaturdayEvening", "k1", "{\"xyz\":fd,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995ancidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"20a19-05-07T14:22:35.863513-08:00\"}")
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"12-11T14:22:35.863513-08:00\"")
    sendMessage("SaturdayEvening", "k1", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"tsfad\":\"2016-12-11T14:22:35.863513-08:00\"}")

    val dir = "src/test/resources/generated_config"
    val fileName = s"$dir/kafka_gcs.properties"
    new File(dir).mkdirs()
    new PrintWriter(fileName) { write(configWithGCSPrefix); close }

    val portalId = MainApp.castPortal(fileName)

    val myRDD1 = sc.textFile(s"gs://$gcsTestPath/SaturdayEvening/2014/05/07/0_0.gz")
    val local1 = myRDD1.toLocalIterator
    assert(local1.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2014-05-07T14:22:35.863513-08:00\"}")


    val myRDD2 = sc.textFile(s"gs://$gcsTestPath/SaturdayEvening/2016/11/27/0_0.gz")
    val local2 = myRDD2.toLocalIterator
    assert(local2.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-11-27T14:22:35.863513-08:00\"}")

    val myRDD3 = sc.textFile(s"gs://$gcsTestPath/SaturdayEvening/2019/05/07/0_0.gz")
    val local3 = myRDD3.toLocalIterator
    assert(local3.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2019-05-07T14:22:35.863513-08:00\"}")

    val myRDD4 = sc.textFile(s"gs://$gcsTestPath/SaturdayEvening/2016/12/12/0_0.gz")
    val local4 = myRDD4.toLocalIterator
    assert(local4.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863513-08:00\"}")
    assert(local4.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863000-08:00\"}")

    val myRDD5 = sc.textFile(s"gs://$gcsTestPath/SaturdayEvening/1969/12/31/0_0.gz")
    val local5 = myRDD5.toLocalIterator
    assert(local5.next() == "{\"xyz\":fd,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995ancidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"20a19-05-07T14:22:35.863513-08:00\"}")
    assert(local5.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"12-11T14:22:35.863513-08:00\"")
    assert(local5.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"tsfad\":\"2016-12-11T14:22:35.863513-08:00\"}")
    shutDownKafka()
  }

  private def configWithGCSPrefix = {
s"""
   |#===================================================================================================
   |# Blink portal configurations
   |#===================================================================================================
   |blink.portal.factory.class=com.creditkarma.blink.factory.KafkaImportPortalFactoryStringType
   |blink.portal.factory.properties.id=kafka-test2
   |blink.portal.factory.properties.kafka.bootstrap.servers=localhost:1234
   |blink.portal.factory.properties.kafka.whitelist="SaturdayEvening"
   |blink.portal.factory.properties.writer.creator.class=com.creditkarma.blink.impl.spark.exporter.kafka.gcs.GCSWriterCreator
   |#this is just some custom property going to the injected class
   |blink.portal.factory.properties.writer.creator.properties.p1=v1
   |blink.portal.factory.properties.zookeeper.host=localhost:5678
   |blink.portal.factory.properties.flush.interval=1000
   |blink.portal.factory.properties.flush.size=1
   |
 |#===================================================================================================
   |# Client configurations -- GCS Writer
   |#===================================================================================================
   |blink.portal.factory.properties.writer.creator.properties.tsName=ts
   |blink.portal.factory.properties.writer.creator.properties.ifWithMicro=true
   |blink.portal.factory.properties.writer.creator.properties.enforcedFields=
   |blink.portal.factory.properties.writer.creator.properties.credentialsPath=testingcredentials/DataScience-f7d364638ad4.json
   |blink.portal.factory.properties.writer.creator.properties.connectTimeoutMs=10000
   |blink.portal.factory.properties.writer.creator.properties.readTimeoutMs=10000
   |blink.portal.factory.properties.writer.creator.properties.bucketName=dataeng_test
   |blink.portal.factory.properties.writer.creator.properties.outputAppString=application/gz
   |blink.portal.factory.properties.writer.creator.properties.metaData=priority,high;period,60
   |blink.portal.factory.properties.writer.creator.properties.cacheControl=
   |blink.portal.factory.properties.writer.creator.properties.outputFileExtension=gz
   |blink.portal.factory.properties.writer.creator.properties.pathPrefix=${gcsPrefix}
   |blink.portal.factory.properties.writer.creator.properties.compression=true
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
