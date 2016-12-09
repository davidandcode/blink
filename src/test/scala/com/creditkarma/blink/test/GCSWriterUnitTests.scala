package com.creditkarma.blink.test

import com.creditkarma.blink.impl.spark.exporter.kafka.{KafkaMessageId, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.creditkarma.blink.impl.spark.exporter.kafka.gcs.{GCSSubPartition, GCSWriter}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
  * Created by shengwei.wang on 12/7/16.
  */
object GCSWriterUnitTests {


  def main(args: Array[String]): Unit = {


    val mGCSWriter: GCSWriter = new GCSWriter(
      "ts",
      true,
      "",
      "/Users/shengwei.wang/projects/DataScience-f7d364638ad4.json",
      10000,
      10000,
      "dataeng_test",
      "application/json",
      "",
      ""
    )


    //val testString:String = "2015-01-07T14:22:35.863513-08:00" //"2015-03-00T00:00:00-0800"
    val testString: String = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-01-07T14:22:35.863513-08:00\"}"

    val mGCSSubPartition: GCSSubPartition = mGCSWriter.getSubPartition(testString)

    println(mGCSSubPartition.getYear)
    println(mGCSSubPartition.getMonth)
    println(mGCSSubPartition.getDay)
    println(mGCSSubPartition.getHour)
    println(mGCSSubPartition.getMinute)
    println(mGCSSubPartition.getSecond)

    val testString2: String = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"1991-01-07T14:22:35.8613-0800\"}"

    val mGCSSubPartition2: GCSSubPartition = mGCSWriter.getSubPartition(testString2)

    println(mGCSSubPartition2.getYear)
    println(mGCSSubPartition2.getMonth)
    println(mGCSSubPartition2.getDay)
    println(mGCSSubPartition2.getHour)
    println(mGCSSubPartition2.getMinute)
    println(mGCSSubPartition2.getSecond)


    val testString3: String = "{\"db3-10:00-0800\",iad1.ckint.io\",\"schemaName\":\"Accident.json\",\",\"ts\":\"1991-01-07T14:22:35.8613-0800\"}"

    val mGCSSubPartition3: GCSSubPartition = mGCSWriter.getSubPartition(testString3)

    println(mGCSSubPartition3.getYear)
    println(mGCSSubPartition3.getMonth)
    println(mGCSSubPartition3.getDay)
    println(mGCSSubPartition3.getHour)
    println(mGCSSubPartition3.getMinute)
    println(mGCSSubPartition3.getSecond)


    val testTopicPartition: TopicPartition = new TopicPartition("testTopic", 5)
    val fakeKMI = KafkaMessageId(testTopicPartition,1)
    val data: mutable.MutableList[KafkaMessageWithId[String, String]] = new mutable.MutableList[KafkaMessageWithId[String, String]]

    val singleRecord1 = new KafkaMessageWithId[String,String]("testKey", "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-01-07T14:22:35.863513-08:00\"}", fakeKMI)
    val year = mGCSWriter.getSubPartition(singleRecord1.value).getYear
    val month = mGCSWriter.getSubPartition(singleRecord1.value).getMonth
    val day = mGCSWriter.getSubPartition(singleRecord1.value).getDay
    data += singleRecord1
    data += new KafkaMessageWithId[String,String]("testKey", "test 2nd line", fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", "test 3rd line", fakeKMI)
    data += new KafkaMessageWithId[String,String]("testKey", "test 4th line", fakeKMI)
    val subPartition = SubPartition(OffsetRange(testTopicPartition, 1234567, 2345678), Some(new GCSSubPartition(year,month,day,"","","")))
    val result: WorkerMeta = mGCSWriter.write(subPartition, data.toIterator)




  }
}