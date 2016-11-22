package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait CheckpointService[C <: Checkpoint[_ , C]] extends Module {
  def commitCheckpoint(cp: C): Unit
  def lastCheckpoint(): C

  final def executeCommit(cp: C): Unit = {
    phaseStarted(Phase.CheckpointCommit)
    Try(commitCheckpoint(cp))
    match {
      case Success(_) =>
        phaseCompleted(Phase.CheckpointCommit)
      case Failure(f) => throw f
    }
  }

  final def executeLoad(): C = {
    phaseStarted(Phase.CheckpointLoad)
    Try(lastCheckpoint())
    match {
      case Success(cp) =>
        phaseCompleted(Phase.CheckpointLoad)
        cp
      case Failure(f) => throw f
    }
  }

  override def moduleType: ModuleType.Value = ModuleType.CheckpointService
}
