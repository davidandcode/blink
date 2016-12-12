package com.creditkarma.blink.test

import com.creditkarma.blink.MainApp
import com.creditkarma.blink.factory.KafkaExportWorkerCreator
import com.creditkarma.blink.impl.spark.exporter.kafka.gcs.{GCSSubPartition, GCSWriter}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
  * Created by shengwei.wang on 12/9/16.
  */
class BlinkGCSWriterIntegrationTest extends FlatSpec with KafkaIntegrationTest[String, String] {

  val conf = new SparkConf().setAppName("spark gcs connector test").setMaster("local[4]")
  val sc = new SparkContext(conf)

  sc.hadoopConfiguration.set("fs.gs.project.id", "295779567055")
  sc.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.email", "dataeng-test@modular-shard-92519.iam.gserviceaccount.com")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", "testingcredentials/DataScience-ac040bae47fb.p12")
  override val configuredPorts: Option[(Int, Int)] = Some((5678, 1234))

  "A writer" should "upload the data/message correctly" in {

    // LogManager.getLogger("org.apache").setLevel(Level.WARN)
   //  LogManager.getLogger("kafka").setLevel(Level.WARN)
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

     val portalId = MainApp.castPortal("src/test/resources/kafka.test3.properties")

    val myRDD1 = sc.textFile("gs://dataeng_test/SaturdayEvening/0/2014/05/07/0.json")
    val local1 = myRDD1.toLocalIterator
    assert(local1.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2014-05-07T14:22:35.863513-08:00\"}")


    val myRDD2 = sc.textFile("gs://dataeng_test/SaturdayEvening/0/2016/11/27/0.json")
    val local2 = myRDD2.toLocalIterator
    assert(local2.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-11-27T14:22:35.863513-08:00\"}")

    val myRDD3 = sc.textFile("gs://dataeng_test/SaturdayEvening/0/2019/05/07/0.json")
    val local3 = myRDD3.toLocalIterator
    assert(local3.next() =="{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2019-05-07T14:22:35.863513-08:00\"}")

    val myRDD4 = sc.textFile("gs://dataeng_test/SaturdayEvening/0/2016/12/12/0.json")
    val local4 = myRDD4.toLocalIterator
    assert(local4.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863513-08:00\"}")
    assert(local4.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2016-12-12T14:22:35.863000-08:00\"}")

    val myRDD5 = sc.textFile("gs://dataeng_test/SaturdayEvening/0/1969/12/31/0.json")
    val local5 = myRDD5.toLocalIterator
    assert(local5.next() == "{\"xyz\":fd,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995ancidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"20a19-05-07T14:22:35.863513-08:00\"}")
    assert(local5.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"12-11T14:22:35.863513-08:00\"")
    assert(local5.next() == "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"tsfad\":\"2016-12-11T14:22:35.863513-08:00\"}")
            shutDownKafka()

    }
}


class GCSTestWriterCreator extends KafkaExportWorkerCreator[String,String,GCSSubPartition] {
  override def writer: GCSWriter = {

    val tsName = getOrFail("tsName")
    val ifWithMirco = getOrFail("ifWithMicro").toBoolean
    val enforcedFields = getOrFail("enforcedFields")
    val credentialsPath = get("credentialsPath").getOrElse("")
    val connectTimeoutMs = get("connectTimeoutMs").getOrElse("10000").toInt
    val readTimeoutMs = get("readTimeoutMs").getOrElse("10000")toInt
    val bucketName = get("bucketName").getOrElse("")
    val outputAppString = get("outputAppString").getOrElse("")
    val metaData = get("metaData").getOrElse("")
    val cacheControl = get("cacheControl").getOrElse("")
    val outputFileExtension = get("outputFileExtension").getOrElse("")

    new GCSWriter(
      tsName,
      ifWithMirco,
      enforcedFields,
      credentialsPath,
      connectTimeoutMs,
      readTimeoutMs,
      bucketName,
      outputAppString,
      metaData,
      cacheControl,
      outputFileExtension,
      ""
    )



  }
}