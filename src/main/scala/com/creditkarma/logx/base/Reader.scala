package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * @tparam Meta meta data of the read operation, used to construct checkpoint delta and metrics
  */
trait Reader[B <: BufferedData, C <: Checkpoint[D, C], D, Meta] extends Module {
  override def moduleType: ModuleType.Value = ModuleType.Reader

  def start(): Unit = {}
  def close(): Unit = {}

  /**
    * Fetch data from checkpoint all the way to the head of the stream
    * In case of back filling with a big time window, the data may be very large, it's the writer's responsibility to properly write them
    * Certain complicated transformation (involving aggregation) may also require prohibitive resources for large inputs
    * Depending on the implementation, this method can potentially fetch data into buffer until it meets the flush condition
    * When using lazy read such as in Spark, there is no need to deal with buffering at read time, but only about meta data
    * @param checkpoint
    * @return the delta of the fetched data
    *
    */
  def fetchData(lastFlushTime: Long, checkpoint: C): (B, Meta)

  /**
    * This is about streaming flush policy, can be based on data size, time interval or combination
    * @return whether the currently fetched data should be send down the stream
    */
  def flush(lastFlushTime: Long, meta: Meta): Boolean

  def inRecords(meta: Meta): Long
  def inBytes(meta: Meta): Long

  def getDelta(meta: Meta): D

  final def execute(lastFlushTime: Long, checkpoint: C): (B, D, Boolean) = {
    phaseStarted(Phase.Read)
    Try(fetchData(lastFlushTime, checkpoint))
    match {
      case Success((data, meta)) =>
        metricUpdate(
          Map(
            MetricArgs.InRecords->inRecords(meta),
            MetricArgs.InBytes->inBytes(meta)
          )
        )
        phaseCompleted(Phase.Read)
        (data, getDelta(meta), flush(lastFlushTime, meta))
      case Failure(f) => throw f
    }
  }
}
