package com.creditkarma.blink.factory

import com.creditkarma.blink.PortalConstructor
import com.creditkarma.blink.base.PortalController
import com.creditkarma.blink.impl.spark.exporter.kafka.ExportWorker
import com.creditkarma.blink.impl.spark.importer.kafka.KafkaTopicFilter
import com.creditkarma.blink.impl.spark.tracker.kafka.ZooKeeperStateTracker
import com.creditkarma.blink.instrumentation.LogInfoToFileInstrumentor
import kafka.consumer.{Blacklist, Whitelist}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Create portals from Kafka specifically
  * The witer class an be injected as a dynamically loaded class
  */
trait KafkaImportPortalFactory[K, V, P] extends PortalFactory {

  val PORTAL_ID_PARAM = "id"
  val KAFKA_SERVERS_PARAM = "kafka.bootstrap.servers"
  val KAFKA_WRITER_CREATOR_PARAM = "writer.creator.class"
  val ZOOKEEPER_HOST_PARAM = "zookeeper.host"
  val FLUSH_INTERVAL_PARAM = "flush.interval"
  val FLUSH_SIZE_PARAM = "flush.size"
  val KAFKA_WHITELIST_PARAM = "kafka.whitelist"
  val KAFKA_BLACKLIST_PARAM = "kafka.blacklist"
  val LOG4J_MAX_FILE_SIZE = "log4j.maxFileSize"
  val LOG4J_MAX_BK_INDEX = "log4j.MaxBackupIndex"
  val LOG4J_FILE_NAME = "log4j.fName"
  val LOG4J_FILE_PATTERN = "log4j.pattern"

  override def build(): PortalController = {
    val writerCreator = Class.forName(getOrFail(KAFKA_WRITER_CREATOR_PARAM)).newInstance().asInstanceOf[KafkaExportWorkerCreator[K, V, P]]
    val writerParamPrefix = "writer.creator.properties."
    for(
      (key, value) <- getPropertiesByPrefix(writerParamPrefix);
      writerParamName = key.substring(writerParamPrefix.length)
    ){
      writerCreator.set(writerParamName, value)
    }
    PortalConstructor.createKafkaSparkPortalWithSingleThreadedWriter(
      name=getOrFail(PORTAL_ID_PARAM),
      kafkaParams = Map[String, Object](
        "bootstrap.servers" -> getOrFail(KAFKA_SERVERS_PARAM),
        "key.deserializer" -> classOf[StringDeserializer], // TODO: all these parameters can be open to config
        "value.deserializer" -> classOf[StringDeserializer],
        // group.id does not matter since this is only used for meta-data request
        // spark will create another consumer group to fetch data from executor threads
        "group.id" -> s"blink.portal.${getOrFail(PORTAL_ID_PARAM)}"
      ),
      singleThreadPartitionWriter = writerCreator.writer,
      checkpointService = new ZooKeeperStateTracker(getOrFail(ZOOKEEPER_HOST_PARAM)),
      flushInterval = getLongOrFail(FLUSH_INTERVAL_PARAM),
      flushSize = getLongOrFail(FLUSH_SIZE_PARAM),
      Seq(LogInfoToFileInstrumentor(getOrFail(LOG4J_MAX_FILE_SIZE),getOrFail(LOG4J_MAX_BK_INDEX),getOrFail(LOG4J_FILE_NAME),getOrFail(LOG4J_FILE_PATTERN))), // instrumentation should also be controllable by parameters, for now just always add the logging one
      new KafkaTopicFilter(get(KAFKA_WHITELIST_PARAM).map(new Whitelist(_)), get(KAFKA_BLACKLIST_PARAM).map(new Blacklist(_)))
    )
  }
}

trait KafkaExportWorkerCreator[K, V, P] extends SettableProperties {
  def writer: ExportWorker[K, V, P]
}

class KafkaImportPortalFactoryStringType extends KafkaImportPortalFactory[String, String, String]
