package com.creditkarma.blink.impl.checkpointservice

import com.creditkarma.blink.Serializer
import com.creditkarma.blink.base.{Checkpoint, CheckpointService}
import com.creditkarma.blink.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.blink.utils.gcs.ZookeeperCpUtils
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by shengwei.wang on 11/19/16.
  */
class ZooKeeperCPService(hostport:String) extends  CheckpointService[KafkaCheckpoint] {

  private val PREFIX = "LASTTIME"

  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {

    val checkpointTopicName = PREFIX + portalId

    ZookeeperCpUtils.znodeExists("/" + checkpointTopicName,hostport) match {
      case false =>{ZookeeperCpUtils.create("/" + checkpointTopicName,  SerializationUtils.serialize(cp.timestampedOffsetRanges.toList),hostport)}
      case true => {ZookeeperCpUtils.update("/" + checkpointTopicName,  SerializationUtils.serialize(cp.timestampedOffsetRanges.toList),hostport)}

    }

  }

  override def lastCheckpoint(): Option[KafkaCheckpoint] = {

    val checkpointTopicName = PREFIX + portalId

    ZookeeperCpUtils.znodeExists("/" + checkpointTopicName ,hostport) match {
      case false => None
      case true => Some( new KafkaCheckpoint(SerializationUtils.deserialize(ZookeeperCpUtils.getDataBytes("/" + checkpointTopicName, hostport))))
     }

    }

}




