package com.creditkarma.blink.base

import scala.util.{Failure, Success, Try}

trait ExportMeta[Delta] extends Metrics{
  /**
    *
    * @return optionally return delta for partial checkpoint
    *         If none is returned, checkpoint will be all(on success) or nothing(on failure)
    */
  def delta: Option[Delta] = None
  def outRecords: Long
}

/**
  * [[Exporter]] flushes stream buffer into the sink
  * @tparam D specific to the reading source's checkpoint, writer can oprtionally return delta for partial commit to checkpoint
  * @tparam Meta meta data of the read operation, used to construct checkpoint delta and metrics
  */
trait Exporter[B <: BufferedData, C <: Checkpoint[D, C], D, Meta <: ExportMeta[D]] extends CoreModule {
  def start(): Unit = {}

  def close(): Unit = {}

  /**
    *
    * @param data           Data in the buffer to be flushed
    * @return The delta successfully written for the purpose of checkpoint. If all data are written, it's the same as delta
    *         Some data in the buffer may not be written for 2 reasons:
    *         1. external failure: writing to the sink may fail
    *         2. internal buffering: to avoid having too many small files, writer may decided to keep the data locally buffered
    *         until it meets the flushing criteria
    *         In case of Kafka Spark RDD, local buffering is simply a matter of manipulating the Kafka OffsetRanges since everything is lazy
    *         The detailed metrics should also be reflected in the writer's implementation
    */
  def export(data: B, sharedState: ExporterAccessor[C, D]): Meta


  final def execute(data: B, sharedState: ExporterAccessor[C, D]): Unit = {
    phaseStarted(Phase.Write)
    Try(export(data, sharedState))
    match {
      case Success(meta) =>
        updateMetrics(meta)
        phaseCompleted(Phase.Write)
        sharedState.setExporterRecords(meta.outRecords)
        sharedState.setExporterDelta(meta.delta)
      case Failure(f) => throw f
    }
  }

  override def moduleType: ModuleType.Value = ModuleType.Writer
}
