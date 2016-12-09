package com.creditkarma.blink.base

import scala.util.{Failure, Success, Try}

/**
  * There is only one [[Portal]] constructor for instantiation, based on the principle of single point creation.
  * Extending the [[Portal]] class is disabled by design.
  * All Higher level creation APIs must originate from this single constructor.
  * The type parameter signature captures the subtle interrelation among the modules.
  * All types, including bounds of type parameters, are Scala traits for maximum extendability.
  * There are no state variables other than the [[stateTracker]].
  * @param id
  * @param reader
  * @param transformer
  * @param writer
  * @param stateTracker
  * @tparam I
  * @tparam O
  * @tparam C
  * @tparam Delta
  */
final class Portal[I <: BufferedData, O <: BufferedData, C <: Checkpoint[Delta, C], Delta]
(
  val id: String, val tickTime: Long,
  reader: Importer[I, C, Delta, _],
  transformer: Transformer[I, O],
  writer: Exporter[O, C, Delta, _],
  stateTracker: StateTracker[C]
) extends CoreModule with PortalController{

  /**
    * this should be a globally unique name for identification purposes,
    * such as avoid conflicts of multiple Portal instances that share global resources such as checkpoint storage.
    * another scenario is for monitoring the entire Portal network, each portal must have a unique identifier
    * The name can be programmatically generated based on various strategies such as using the source/sink details.
    * Regardless how it is generated, it must be provided to the constructor.
    */
  this.registerPortal(id)
  reader.registerPortal(id)
  transformer.registerPortal(id)
  writer.registerPortal(id)
  stateTracker.registerPortal(id)

  override def moduleType: ModuleType.Value = ModuleType.Core
  override def registerInstrumentor(instrumentor: Instrumentor): Unit = {
    super.registerInstrumentor(instrumentor)
    reader.registerInstrumentor(instrumentor)
    transformer.registerInstrumentor(instrumentor)
    writer.registerInstrumentor(instrumentor)
    stateTracker.registerInstrumentor(instrumentor)
  }

  /**
    *
    * @return whether any data is flushed in this cycle,
    *         if true, it's likely there are more data ready to be pushed, although there can be quite a few scenarios
    *         A simple solution is to immediately run anothe cycle without waiting for the polling interval.
    */
  private def loadCheckpointAndThen(sharedState: ModuleSharedState[C, Delta]): Unit = {
    Try(stateTracker.executeLoad(sharedState.asInstanceOf[StateTrackerAccessor[C, Delta]])) match {
      case Success(lastCheckpoint) =>
        updateStatus(new StatusOK(s"got last checkpoint ${lastCheckpoint}"))
        readAndThen(sharedState)
      case Failure(f) =>
        updateStatus(new StatusError(f, "checkpoint load failure"))
    }
  }

  private def readAndThen(sharedState: ModuleSharedState[C, Delta]): Unit = {
    Try(reader.execute(sharedState.asInstanceOf[ImporterAccessor[C, Delta]])) match {
      case Success(inData) =>
        if(sharedState.importerFlush) {
          updateStatus(new StatusOK(s"ready to flush"))
          transformAndThen(inData, sharedState)
        }
        else{
          updateStatus(new StatusOK("not enough to flush"))
        }
      case Failure(f) =>
        updateStatus(new StatusError(f, "read failure"))
    }
  }

  private def transformAndThen(inData: I, sharedState: ModuleSharedState[C, Delta]): Unit = {
    Try(transformer.execute(inData)) match {
      case Success(outData) =>
        writeAndCommitCheckpoint(outData, sharedState)
      case Failure(f) =>
        updateStatus(new StatusError(f, "transform failure"))
    }
  }

  private def writeAndCommitCheckpoint(outData: O, sharedState: ModuleSharedState[C, Delta]): Unit = {
    def lastCheckpoint = sharedState.lastCheckpoint
    def inTime = sharedState.importerTime
    def inDelta = sharedState.importerDelta
    Try(writer.execute(outData, sharedState.asInstanceOf[ExporterAccessor[C, Delta]])) match {
      case Success(_) =>
        def outDelta = sharedState.exporterDelta
        def outRecords = sharedState.exporterRecords
        updateStatus(new StatusOK("ready to checkpoint"))
        commitCheckpoint(
          lastCheckpoint
            .mergeDelta(
              outDelta.getOrElse(inDelta), inTime
            )
        )
        outRecords > 0
      case Failure(f)=>
        updateStatus(new StatusError(f, "write failure"))
    }
  }

  private def commitCheckpoint(cp: C): Unit = {
    Try(stateTracker.executeCommit(cp)) match {
      case Success(_) =>
        updateStatus(new StatusOK(s"checkpoint success ${cp}"))
      case Failure(f) =>
        updateStatus(new StatusError(f, "checkpoint commit failure"))
    }
  }

  private def runOneCycle(): Boolean = {
    cycleStarted(this)
    // initialized local mutable state for this cycle
    val sharedState = new ModuleSharedState[C, Delta]
    loadCheckpointAndThen(sharedState)
    cycleCompleted(this)
    // look at information from the mutable state to inform higher level control
    sharedState.exporterRecords > 0
  }

  private def runLoop(): Unit = {
    while (true) {
      runUntilNoPush()
      updateStatus(new StatusOK(s"No data pushed, wait for ${tickTime} ms"))
      Thread.sleep(tickTime)
    }
  }

  private def runUntilNoPush(): Unit = {
    while(runOneCycle()){
      updateStatus(new StatusOK(s"run cycle immediately after data push"))
    }
    updateStatus(new StatusOK(s"No data pushed, stop tight loop"))
  }

  private[this] var tunnelOpened = false
  private def closeTunnel(): Unit = this.synchronized {
    if(tunnelOpened){
      updateStatus(new StatusOK("closing tunnel"))
      reader.close()
      writer.close()
      tunnelOpened = false
      updateStatus(new StatusOK("tunnel is closed"))
    }
    else{
      updateStatus(new StatusOK("tunnel already closed"))
    }
  }

  private def openTunnel(): Unit = this.synchronized {
    if(!tunnelOpened) {
      updateStatus(new StatusOK(s"opening tunnel"))
      reader.start()
      writer.start()
      tunnelOpened = true
      updateStatus(new StatusOK(s"tunnel is open"))
    }
    else{
      updateStatus(new StatusOK("tunnel already opened"))
    }
  }

  //TODO
  //Global portal network state update
  private def closePortal(): Unit = {
    updateStatus(new StatusOK("closing portal"))
    closeTunnel()
    updateStatus(new StatusOK("portal is closed"))
  }

  //TODO
  //Global portal network state update
  override def openPortal(operation: OperationMode.Value, time: TimeMode.Value): Unit = {
    updateStatus(new StatusOK(s"opening portal with operation=${operation}, and time=${time}"))
    // first open the funnel. TODO: may also need to test the tunnel (importer, exporter, transformer) before starting the loop, in case of misconfiguration
    openTunnel()
    // then initialize checkpoint based on time mode
    time match {
      case TimeMode.Origin =>
        val checkpoint = reader.checkpointFromEarliest()
        updateStatus(new StatusOK(s"rewind checkpoint to earliest: cp=[${checkpoint}]"))
        stateTracker.persist(checkpoint)
      case TimeMode.Now =>
        val checkpoint = reader.checkpointFromNow()
        updateStatus(new StatusOK(s"mark current position: cp=[${checkpoint}]"))
        stateTracker.persist(checkpoint)
      case TimeMode.MemoryOrNow =>
        stateTracker.lastCheckpoint() match {
          case Some(checkpoint) =>
            updateStatus(new StatusOK(s"use last checkpoint position: cp=[${checkpoint}]"))
          case None =>
            val checkpoint = reader.checkpointFromNow()
            updateStatus(new StatusOK(s"not checkpoint found, mark current position: cp=[${checkpoint}]"))
            stateTracker.persist(checkpoint)
        }
      case TimeMode.MemoryOrFail =>
        stateTracker.lastCheckpoint() match {
          case Some(checkpoint) =>
            updateStatus(new StatusOK(s"use last checkpoint position: cp=[${checkpoint}]"))
          case None =>
            throw new Exception(s"no checkpoint found")
        }
    }
    // start portal operation based on operation mode, also close portal accordingly
    operation match {
      case OperationMode.Forever =>
        updateStatus(new StatusOK(s"portal operates forever, but is at the mercy of the ruler of universe"))
        scala.sys.addShutdownHook({
          updateStatus(new StatusOK(s"portal is permanent, but destroyed by the ruler of universe"))
          closePortal()
        })
        runLoop()
      case OperationMode.ImporterDepletion =>
        updateStatus(new StatusOK(s"portal operation started, and will close automatically when input is depleted"))
        runUntilNoPush()
        //TODO must then flush the remaining data from input source
        closePortal()
    }
  }
}

/**
  * This is the public API for launching a portal
  */
sealed trait PortalController {
  def openPortal(operation: OperationMode.Value, time: TimeMode.Value): Unit
  def portalId: String
}

object OperationMode extends Enumeration {
  val Forever, ImporterDepletion = Value
}

/**
  * In addition to bridging across space, a portal can also go back in time.
  * The supported mode are
  * 1. Origin: go back as far as possible, use for back-filling
  * 2. Now: start from the current head of [[Importer]] source
  * 4. MemoryOrNow: start from the last memorized checkpointed position by [[StateTracker]]. If not exists, start from now.
  */
object TimeMode extends Enumeration {
  val Origin, Now, MemoryOrFail, MemoryOrNow = Value
}
