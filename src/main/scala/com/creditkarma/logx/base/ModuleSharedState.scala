package com.creditkarma.logx.base

/**
  * Mutable state to share information among modules within each cycle.
  * Individual traits will be used to control read/write access for each module.
  * This is for information that needs to be accessed across modules that are not executed next to each other,
  * so there is no way to chain the operations in functional style.
  * Such information include:
  * 1. lastCheckpoint: reader must access it which is next to the checkpoint service module. But transformer and writer potentially also needs to access it
  * 2. at the end of each cycle, the control logic may need to look at states across several modules to decide what to do next.
  * This is also for better organizing the module returned information in one place and with a standardized interface,
  * so the portal can be extended with more sophisticated control logic.
  * Using shared state is only for better extendability, but it is implicit and there is the trade-off between explicitly returning in functional style.
  */
class ModuleSharedState[C <: Checkpoint[Delta, C], Delta]
  extends ModuleStateReader[C, Delta]
    with StateTrackerAccessor[C, Delta]
    with ImporterAccessor[C, Delta]
    with TransformerAccessor[C, Delta]
    with ExporterAccessor[C, Delta]{
  private var _lastCheckpoint: Option[C] = None

  private var _importerDelta: Option[Delta] = None
  private var _importerTime: Option[Long] = None
  private var _importerFlush: Option[Boolean] = None

  private var _exporterDelta: Option[Delta] = None
  private var _exporterRecords: Option[Long] = None

  // Getters
  override def lastCheckpoint: C = {
    if(_lastCheckpoint.isEmpty){
      throw new Exception("lastCheckpoint was not set, the stateTracker probably forgot to set it in shared state")
    }
    _lastCheckpoint.get
  }

  override def importerDelta: Delta = {
    if(_importerDelta.isEmpty){
      throw new Exception("importerDelta was not set, the importer probably forgot to set it in shared state")
    }
    _importerDelta.get
  }

  override def importerTime: Long = {
    if(_importerTime.isEmpty){
      throw new Exception("importerTime was not set, the importer probably forgot to set it in shared state")
    }
    _importerTime.get
  }

  override def importerFlush: Boolean = {
    if(_importerFlush.isEmpty){
      throw new Exception("importerFlush was not set, the importer probably forgot to set it in shared state")
    }
    _importerFlush.get
  }

  // exporterDelta is optional
  override def exporterDelta: Option[Delta] = _exporterDelta

  // exporterRecords is optional as phase may be skipped directly
  override def exporterRecords: Long = _exporterRecords.getOrElse(0L)

  // Setters
  override def setLastCheckpoint(checkpoint: C): Unit = {_lastCheckpoint = Option(checkpoint)}

  override def setImporterDelta(delta: Delta): Unit = {_importerDelta = Option(delta)}

  override def setImporterTime(time: Long): Unit = {_importerTime = Option(time)}

  override def setImporterFlush(flush: Boolean): Unit = {_importerFlush = Option(flush)}

  override def setExporterDelta(delta: Option[Delta]): Unit = {_exporterDelta = delta}

  override def setExporterRecords(records: Long): Unit = {_exporterRecords = Option(records)}
}

trait ModuleStateReader[C <: Checkpoint[Delta, C], Delta] {
  def lastCheckpoint: C
  def importerDelta: Delta
  def importerTime: Long
  def importerFlush: Boolean
  def exporterDelta: Option[Delta]
  def exporterRecords: Long
}

trait StateTrackerAccessor[C <: Checkpoint[Delta, C], Delta] extends ModuleStateReader[C, Delta]{
  def setLastCheckpoint(checkpoint: C): Unit
}

trait ImporterAccessor[C <: Checkpoint[Delta, C], Delta] extends ModuleStateReader[C, Delta]{
  def setImporterDelta(delta: Delta): Unit
  def setImporterTime(time: Long): Unit
  def setImporterFlush(flush: Boolean): Unit
}

trait TransformerAccessor[C <: Checkpoint[Delta, C], Delta] extends ModuleStateReader[C, Delta]{
}

trait ExporterAccessor[C <: Checkpoint[Delta, C], Delta] extends ModuleStateReader[C, Delta]{
  def setExporterDelta(delta: Option[Delta]): Unit
  def setExporterRecords(records: Long): Unit
}
