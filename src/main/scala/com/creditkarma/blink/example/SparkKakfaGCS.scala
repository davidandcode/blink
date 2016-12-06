package com.creditkarma.blink.example

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}

import com.creditkarma.blink.utils.gcs.GCSUtils
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConverters._


/**
  * Created by yongjia.wang on 11/15/16.
  */
object SparkKafkaGCS {


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.serializer" -> classOf[StringSerializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "com/creditkarma/logx/example",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  ).asJava

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder
      .master("local[2]")
      .appName("Example")
      .getOrCreate()*/
    System.out.println(java.net.InetAddress.getLocalHost)
    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("Test").setMaster("local[2]")) //spark.sparkContext
    sc.hadoopConfiguration.set("fs.gs.project.id", "295779567055")
    sc.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.email", "dataeng-dev@modular-shard-92519.iam.gserviceaccount.com")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", "/Users/yongjia.wang/Documents/GCP/dataeng_test/key/DataScience-d3a4bb3a4685.p12")

    val kc = createKafkaConsumer()
    val offsetRanges = getTopicOffsetRanges(kc)
    kc.close()
    println(offsetRanges)


    val kafkaRDD: RDD[ConsumerRecord[String, String]] =
      KafkaUtils.createRDD[String, String](
        sc, // spark context
        kafkaParams,
        offsetRanges.filter(_.topic=="topicA").toArray, //message ranges
        PreferConsistent // location strategy
      )


    val t1 = System.nanoTime()
    val taskTimes: Seq[(Int, Long)] =
      kafkaRDD.mapPartitionsWithIndex { // each map partition corresponds to a Kafka topic partition
        case (index: Int, input: Iterator[ConsumerRecord[String, String]]) =>
          val tt1 = System.nanoTime()
          val request =
            GCSUtils
              .getService( // get gcs storage service
                "/Users/yongjia.wang/Projects/github/secor/secor_install/credential/DataScience-f7d364638ad4.json",
                10000, // connection timeout
                10000 // read timeout
              )
              .objects.insert( // insert object
              "dataeng_test", // gcs bucket
              new StorageObject().setName(s"ywang/spark_kafka_direct_gcs4/${index}.json"), // gcs object name
              new InputStreamContent(
                "application/json",
                iteratorToStream(
                  input.map{
                    record: ConsumerRecord[String, String] => record.value() + "\n"
                    // don't forget add new line for each string
                  }
                )
              )
            )
          request.getMediaHttpUploader.setDirectUploadEnabled(true)
          request.execute()
          val tt2 = System.nanoTime()
          // send task statistics for this partition back to driver
          Seq((index, tt2 - tt1)).iterator
      }.collect()
    val t2 = System.nanoTime()
    println(s"Took ${(t2 - t1) / 1000000} mseconds ${taskTimes.toSeq}")
    /*
    output：
    Took 47827 mseconds WrappedArray((0,19715325264), (1,17785353660), (2,24099243938), (3,13886833072), (4,13780239772))
    */

    sc.hadoopConfiguration.set("fs.gs.project.id", "295779567055")
    sc.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
    sc.hadoopConfiguration
      .set(
        "google.cloud.auth.service.account.email",
        "dataeng-test@modular-shard-92519.iam.gserviceaccount.com")
    sc.hadoopConfiguration
      .set(
        "google.cloud.auth.service.account.keyfile",
        "/Users/yongjia.wang/Documents/GCP/dataeng_test/key/DataScience-d3a4bb3a4685.p12")

    //kafkaRDD.saveAsTextFile("gs://dataeng_test/ywang/spark_kafka_gcs_test")

    /*def sparkTransform [T <:Product, U<:Product](input: Dataset[T], transformFunc: T=>U): Dataset[U] = {
      input.map(transformFunc)
    }*/

  }

  def getTopicOffsetRanges(kc: KafkaConsumer[_,_], from:Seq[OffsetRange] = Seq.empty): Seq[OffsetRange] = {

    val topicPartitions = kc.listTopics().asScala.flatMap(_._2.asScala).map{
      pi => new TopicPartition(pi.topic(), pi.partition())
    }.toSeq

    kc.assign(topicPartitions.asJava) // initialize empty partition offset to 0, otherwise it'll through Exception
    kc.seekToBeginning(topicPartitions.asJava)

    val topicPartitionEarliestOffSets = topicPartitions.map{
      tp => (tp, kc.position(tp))
    }.toMap

    kc.seekToEnd(topicPartitions.asJava)

    // if the from offsetRange presents for a topic-partition, use the untilOffset
    val fromOffsetMap = from.map{
      offsetRange => offsetRange.topicPartition() -> offsetRange.untilOffset
    }.toMap

    for(
      tp <- topicPartitions;
      earliestOffset: Long =
      fromOffsetMap.getOrElse(tp, // first try from offset, then try earliest offset
        topicPartitionEarliestOffSets.getOrElse(tp, 0L));
      latestOffset: Long = kc.position(tp)
    ) yield {
      OffsetRange(tp, earliestOffset, latestOffset)
    }
  }

  def createKafkaConsumer() = {

    new KafkaConsumer[String, String](kafkaParams)
  }


  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => new ByteArrayInputStream(s.getBytes("UTF-8")) }
      i.asJavaEnumeration
    })
  }
}


class IteratorInputStream(input: Iterator[String]) extends InputStream {

  var _currentString: String = null
  var _currentIndex = 0
  override def read(): Int = {
    if(_currentString == null && input.hasNext){
      _currentString = input.next()
      _currentIndex = 0
    }

    if(input.hasNext){
      input.next().toInt
    }
    else{
      -1
    }
  }
}