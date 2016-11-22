package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Writer flushes stream buffer into the sink
  * @tparam Delta specific to the reading source, writer can oprtionally return delta for partial commit to checkpoint
  * @tparam Meta meta data of the read operation, used to construct checkpoint delta and metrics
  */
trait Writer[B <: BufferedData, Delta, Meta] extends Module {
  def start(): Unit = {}
  def close(): Unit = {}
  /**
    *
    * @param data Data in the buffer to be flushed
    * @return The delta successfully written for the purpose of checkpoint. If all data are written, it's the same as delta
    */
  def write(data: B): Meta
  def inBytes(meta: Meta): Long
  def inRecords(meta: Meta): Long
  def outBytes(meta: Meta): Long
  def outRecords(meta: Meta): Long

  /**
    *
    * @param meta
    * @return optionally return delta for partial checkpoint
    *         If none is returned, checkpoint will be all(on success) or nothing(on failure)
    */
  def getDelta(meta: Meta): Option[Delta] = None

  final def execute(data: B): Option[Delta] = {
    phaseStarted(Phase.Write)
    Try(write(data))
    match {
      case Success(meta) =>
        metricUpdate(
          Map(
            MetricArgs.InRecords->inRecords(meta),
            MetricArgs.InBytes->inBytes(meta),
            MetricArgs.OutRecords->outRecords(meta),
            MetricArgs.OutBytes->outBytes(meta)
          )
        )
        phaseCompleted(Phase.Write)
        getDelta(meta)
      case Failure(f) => throw f
    }
  }

  override def moduleType: ModuleType.Value = ModuleType.Writer
}
