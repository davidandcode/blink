package com.creditkarma.blink.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait StateTracker[C <: Checkpoint[_ , C]] extends CoreModule {
  def persist(cp: C): Unit
  def lastCheckpoint(): Option[C]

  // caching should be handled by the base trait, client implementation should always go to the persistence storage
  private var cachedCheckpoint: Option[C] = None

  final def executeCommit(cp: C): Unit = {
    phaseStarted(Phase.CheckpointCommit)
    Try(persist(cp))
    match {
      case Success(_) =>
        phaseCompleted(Phase.CheckpointCommit)
        // if persistence failed, the stream should not progress even it can just use in memory checkpoint
        // because the risk is that when the system restarts from an older checkpoint, idempotent output will be broken
        cachedCheckpoint = Some(cp)
      case Failure(f) => throw f
    }
  }

  final def executeLoad(sharedState: StateTrackerAccessor[C, _]): C = {
    phaseStarted(Phase.CheckpointLoad)
    Try(
      if(cachedCheckpoint.nonEmpty){
        updateStatus(new StatusOK(s"using cached checkpoint"))
        cachedCheckpoint
      }
      else {
        // this should only happen when the portal starts, then it will use the cached version
        updateStatus(new StatusOK(s"loading checkpoint from persistent storage"))
        lastCheckpoint()
      })
    match {
      case Success(cp) =>
        phaseCompleted(Phase.CheckpointLoad)
        if(cp.isEmpty){
          throw new Exception(s"No checkpoint loaded")
        }
        sharedState.setLastCheckpoint(cp.get)
        cp.get
      case Failure(f) => throw new Exception("checkpoint service failure", f)
    }
  }

  override def moduleType: ModuleType.Value = ModuleType.CheckpointService
}
