package com.creditkarma.blink.impl.spark.tracker.kafka

import java.util.concurrent.CountDownLatch

import com.creditkarma.blink.base.{StateTracker, StatusOK}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.zookeeper._

import scala.util.{Failure, Success, Try}

/**
  * Persist Kafka checkpoint into Zookeeper.
  * There is a limit of 1M data per zookeeper node.
  * It is not as scalable as using kafka offset commit API to save checkpoints into Kafka topics.
  */
class ZooKeeperStateTracker(hostPort: String) extends StateTracker[KafkaCheckpoint] {

  private val PREFIX = "LASTTIME"
  private val timeOut = 3000

  private def zkNodePath = "/" + PREFIX + portalId // portalId is not available at construction time, so must use def not val

  private def zkOpen = {
    updateStatus(new StatusOK(s"ZooKeeper client connecting $hostPort"))
    val connected = new CountDownLatch(1)
    val zookeeper = new ZooKeeper(hostPort, timeOut, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        connected.countDown()
        updateStatus(new StatusOK(s"ZooKeeper client connected to $hostPort"))
      }
    })
    connected.await()
    zookeeper
  }

  private def zkClose(): Unit = {
    updateStatus(new StatusOK(s"closing ZooKeeper client from $hostPort"))
    _zkClient.foreach(_.close())
    _zkClient = None
    updateStatus(new StatusOK(s"ZooKeeper client closed"))
  }

  private var _zkClient: Option[ZooKeeper] = None
  // zkClient is always safe to use and won't re-open.
  private def zkClient: ZooKeeper =
  _zkClient.getOrElse {
    _zkClient = Option(zkOpen)
    _zkClient.get
  }

  private def saveDataToZK(data: Array[Byte]): Unit = {
    updateStatus(new StatusOK(s"saving ${data.size} bytes"))
    if (!nodeExists) {
      zkClient.create(zkNodePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
    zkClient.setData(zkNodePath, data, -1) // -1 matches any node version
  }

  private def loadDataFromZK(): Array[Byte] = {
    val data = zkClient.getData(zkNodePath, false, null)
    updateStatus(new StatusOK(s"retrieved data: ${data.size} bytes"))
    data
  }

  private def nodeExists(): Boolean = {
    Try(zkClient.exists(zkNodePath, false)) match {
      case Success(stats) => stats != null
      case Failure(f) => throw new Exception(s"Failed to query node", f)
    }
  }


  override def persist(cp: KafkaCheckpoint): Unit = {
    saveDataToZK(SerializationUtils.serialize(cp.timestampedOffsetRanges.toArray))
    zkClose()
  }

  override def lastCheckpoint(): Option[KafkaCheckpoint] = {
    if (nodeExists) {
      val data = loadDataFromZK
      zkClose()
      Some(new KafkaCheckpoint(
        SerializationUtils.deserialize(data)
          .asInstanceOf[Array[(OffsetRange, Long)]])
      )
    }
    else {
      None
    }
  }
}




