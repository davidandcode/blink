package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Writer flushes stream buffer into the sink
  * @tparam D specific to the reading source's checkpoint, writer can oprtionally return delta for partial commit to checkpoint
  * @tparam Meta meta data of the read operation, used to construct checkpoint delta and metrics
  */
trait Writer[B <: BufferedData, C <: Checkpoint[D, C], D, Meta] extends Module {
  def start(): Unit = {}

  def close(): Unit = {}

  /**
    *
    * @param data           Data in the buffer to be flushed
    * @param lastCheckpoint writer may need info saved in last checkpoint to determine which portion of the data to write.
    *                       For example, to guarantee worst case latency and also avoid too many small files,
    *                       writer may flush a kafka topic-partition based on last flush time even the number of records does not meet the threshold.
    * @param inTime         input read time of the current cycle. Compare this time with the time saved with last checkpoint
    *                       yields the data interval of the input source
    * @return The delta successfully written for the purpose of checkpoint. If all data are written, it's the same as delta
    *         Some data in the buffer may not be written for 2 reasons:
    *         1. external failure: writing to the sink may fail
    *         2. internal buffering: to avoid having too many small files, writer may decided to keep the data locally buffered
    *         until it meets the flushing criteria
    *         In case of Kafka Spark RDD, local buffering is simply a matter of manipulating the Kafka OffsetRanges since everything is lazy
    *         The detailed metrics should also be reflected in the writer's implementation
    */
  def write(data: B, lastCheckpoint: C, inTime: Long): Meta

  def getMetrics(meta: Meta): Seq[Map[Any, Any]]

  /**
    *
    * @param meta
    * @return optionally return delta for partial checkpoint
    *         If none is returned, checkpoint will be all(on success) or nothing(on failure)
    */
  def getDelta(meta: Meta): Option[D] = None

  final def execute(data: B, lastCheckpoint: C, inTime: Long): Option[D] = {
    phaseStarted(Phase.Write)
    Try(write(data, lastCheckpoint, inTime))
    match {
      case Success(meta) =>
        updateMetrics(getMetrics(meta))
        phaseCompleted(Phase.Write)
        getDelta(meta)
      case Failure(f) => throw f
    }
  }

  override def moduleType: ModuleType.Value = ModuleType.Writer
}
