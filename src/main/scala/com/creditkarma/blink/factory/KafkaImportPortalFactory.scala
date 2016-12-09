package com.creditkarma.blink.factory

import com.creditkarma.blink.PortalConstructor
import com.creditkarma.blink.base.PortalController
import com.creditkarma.blink.impl.spark.exporter.kafka.ExportWorker
import com.creditkarma.blink.impl.spark.importer.kafka.KafkaTopicFilter
import com.creditkarma.blink.impl.spark.tracker.kafka.ZooKeeperStateTracker
import com.creditkarma.blink.instrumentation.LogInfoInstrumentor
import kafka.consumer.{Blacklist, Whitelist}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalactic.Fail

import scala.util.{Failure, Success, Try}

/**
  * Create portals from Kafka specifically
  * The witer class an be injected as a dynamically loaded class
  */
class KafkaImportPortalFactory extends PortalFactory {

  val PORTAL_ID_PARAM = "id"
  val KAFKA_SERVERS_PARAM = "kafka.bootstrap.servers"
  val KAFKA_WRITER_CREATOR_PARAM = "writer.creator.class"
  val ZOOKEEPER_HOST_PARAM = "zookeeper.host"
  val FLUSH_INTERVAL_PARAM = "flush.interval"
  val FLUSH_SIZE_PARAM = "flush.size"
  val KAFKA_WHITELIST_PARAM = "kafka.whitelist"
  val KAFKA_BLACKLIST_PARAM = "kafka.blacklist"


  override def build(): PortalController = {
    val writerCreator = Class.forName(get(KAFKA_WRITER_CREATOR_PARAM)).newInstance().asInstanceOf[KafkaStringPartitionWriterCreator]
    val writerParamPrefix = "writer.creator.properties."
    for(
      (key, value) <- getPropertiesByPrefix(writerParamPrefix);
      writerParamName = key.substring(writerParamPrefix.length)
    ){
      writerCreator.set(writerParamName, value)
    }
    PortalConstructor.createKafkaSparkPortalWithSingleThreadedWriter(
      name=get(PORTAL_ID_PARAM),
      kafkaParams = Map[String, Object](
        "bootstrap.servers" -> get(KAFKA_SERVERS_PARAM),
        "key.deserializer" -> classOf[StringDeserializer], // TODO: all these parameters can be open to config
        "value.deserializer" -> classOf[StringDeserializer],
        // group.id does not matter since this is only used for meta-data request
        // spark will create another consumer group to fetch data from executor threads
        "group.id" -> s"blink.portal.${get(PORTAL_ID_PARAM)}"
      ),
      singleThreadPartitionWriter = writerCreator.writer,
      checkpointService = new ZooKeeperStateTracker(get(ZOOKEEPER_HOST_PARAM)),
      flushInterval = getLong(FLUSH_INTERVAL_PARAM),
      flushSize = getLong(FLUSH_SIZE_PARAM),
      Seq(LogInfoInstrumentor()), // instrumentation should also be controllable by parameters, for now just always add the logging one
      new KafkaTopicFilter(getOption(KAFKA_WHITELIST_PARAM).map(new Whitelist(_)), getOption(KAFKA_BLACKLIST_PARAM).map(new Blacklist(_)))
    )
  }
}

trait KafkaStringPartitionWriterCreator extends SettableProperties {
  def writer: ExportWorker[String, String, String]
}
