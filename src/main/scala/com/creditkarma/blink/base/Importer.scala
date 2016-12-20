package com.creditkarma.blink.base

import scala.util.{Failure, Success, Try}


trait ImporterMetrics extends Metrics {
  // The importer may decide not to import all the available records, so they are separate metrics
  def inRecords: Long
  def availableRecords: Long
}

trait ImportMeta[D] extends ImporterMetrics {
  def delta: D
  def readTime: Long
  def shouldFlush: Boolean
}

trait Importer[B <: BufferedData, C <: Checkpoint[D, C], D, M <: ImportMeta[D]] extends CoreModule {
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
  def fetchData(checkpoint: C): (B, M)

  /**
    * Reader must correctly interpret checkpoint
    * @return a checkpoint that will make the reader get all available data from source, this is for back filling
    */
  def checkpointFromEarliest(): C

  /**
    *
    * @return a checkpoint that will make reader ignore existing data from the source and start from now
    */
  def checkpointFromNow(): C

  final def execute(sharedState: ImporterAccessor[C, D]): B = {
    phaseStarted(Phase.Read)
    Try(fetchData(sharedState.lastCheckpoint))
    match {
      case Success((data, meta)) =>
        updateMetrics(meta)
        phaseCompleted(Phase.Read)
        sharedState.setImporterDelta(meta.delta)
        sharedState.setImporterTime(meta.readTime)
        sharedState.setImporterFlush(meta.shouldFlush)
        data
      case Failure(f) => throw f
    }
  }
}
