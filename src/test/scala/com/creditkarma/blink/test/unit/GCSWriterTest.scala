package com.creditkarma.blink.test.unit

import com.creditkarma.blink.impl.spark.exporter.kafka.gcs.GCSWriter
import com.creditkarma.blink.impl.spark.exporter.kafka.{KafkaMessageId, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.creditkarma.blink.test.{GCSTest, LocalSpark}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest.FlatSpec

import scala.collection.mutable

/**
  * Created by shengwei.wang on 12/11/16.
  */
class GCSWriterTest extends FlatSpec with LocalSpark with GCSTest{

  val mGCSWriter: GCSWriter = new GCSWriter(
    tsName = "ts",
    partitionFormat = "yyyy/MM/dd",
    ifWithMicro = true,
    enforcedFields = "",
    credentialsPath = "testingcredentials/DataScience-f7d364638ad4.json",
    connectTimeoutMs = 10000,
    readTimeoutMs = 10000,
    bucketName = "dataeng_test",
    outputAppString = "application/json",
    metaData = "priority,high;period,60",
    cacheControl = "",
    outputFileExtension = "json",
    pathPrefix = gcsPrefix,
    compression = false
  )

  sc.hadoopConfiguration.set("fs.gs.project.id", "295779567055")
  sc.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.email", "dataeng-test@modular-shard-92519.iam.gserviceaccount.com")
  sc.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", "testingcredentials/DataScience-ac040bae47fb.p12")


  "A writer" should "get the subpartition properly" in {

    val testString: String = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-01-07T14:22:35.863513-08:00\"}"
    val mGCSSubPartition = mGCSWriter.getSubPartition(testString)
    assert(mGCSSubPartition == "2015/01/07")

    val testString2: String = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"1991-02-08T14:22:35.8613-0800\"}"
    val mGCSSubPartition2 = mGCSWriter.getSubPartition(testString2)
    assert(mGCSSubPartition2 == "1991/02/08")

    var testString3: String = "{\"dwNumericId\":5446427592603205633,\"ts\":\"1994-01-07T14:22:35.8613-0800\"}"
    val mGCSSubPartition3 = mGCSWriter.getSubPartition(testString3)
    assert(mGCSSubPartition3 == "1994/01/07")

  }


  "A writer" should "upload the data/message correctly" in {
    val fakePartition = 5
    val fakeOffset = 1234567

    val testTopicPartition: TopicPartition = new TopicPartition("GCSWriterUnitTest", fakePartition)
    val fakeKMI = KafkaMessageId(testTopicPartition,1)
    val data: mutable.MutableList[KafkaMessageWithId[String, String]] = new mutable.MutableList[KafkaMessageWithId[String, String]]
    val payload1 = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-01T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-11-27T14:22:35.863513-08:00\"}"
    val payload2 = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-01T00:00:00-0800\",\"incidentType\":\"MAJOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-11-27T14:22:35.863513-08:00\"}"
    data += new KafkaMessageWithId[String,String]("testKey", payload1, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", null, fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", payload2, fakeKMI)
    val subPartition = SubPartition(OffsetRange(testTopicPartition, fakeOffset, 2345678), Some(mGCSWriter.getSubPartition(payload1)))
    val result: WorkerMeta = mGCSWriter.write(subPartition, data.toIterator)

    // the data uploaded to gcs may not be immediately available
    val myRDD = sc.textFile(s"gs://$gcsTestPath/GCSWriterUnitTest/2015/11/27/${fakePartition}_${fakeOffset}.json")
    assert(result.complete == true)
    assert(result.records == myRDD.count())
    val localIterator = myRDD.toLocalIterator
    assert(localIterator.next() == payload1)
    assert(localIterator.next() == "null")
    assert(localIterator.next() == payload2)
  }

}
