package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */

class LogXCore[I <: BufferedData, O <: BufferedData, C <: Checkpoint[Delta, C], Delta]
(
  val appName: String,
  reader: Reader[I, C, Delta, _],
  transformer: Transformer[I, O],
  writer: Writer[O, Delta, _],
  checkpointService: CheckpointService[C]
) extends Module {

  override def moduleType: ModuleType.Value = ModuleType.Core
  override def registerInstrumentor(instrumentor: Instrumentor): Unit = {
    super.registerInstrumentor(instrumentor)
    reader.registerInstrumentor(instrumentor)
    transformer.registerInstrumentor(instrumentor)
    writer.registerInstrumentor(instrumentor)
    checkpointService.registerInstrumentor(instrumentor)
  }

  private val DefaultPollingInterval = 1000L

  var lastFlushTime: Long = 0

  private def loadCheckpointAndThen(): Unit = {
    Try(checkpointService.executeLoad()) match {
      case Success(lastCheckpoint) =>
        statusUpdate(new StatusOK(s"got last checkpoint ${lastCheckpoint}"))
        readAndThen(lastCheckpoint)
      case Failure(f) => statusUpdate(new StatusError(f, "checkpoint load failure"))
    }
  }

  private def readAndThen(lastCheckpoint: C): Unit = {
    Try(reader.execute(lastFlushTime, lastCheckpoint)) match {
      case Success((inData, inDelta, flush)) =>
        if (flush) {
          lastFlushTime = System.currentTimeMillis()
          statusUpdate(new StatusOK("ready to flush"))
          transformAndThen(lastCheckpoint, inDelta, inData)
        }
        else{
          statusUpdate(new StatusOK("not enough to flush"))
        }
      case Failure(f) => statusUpdate(new StatusError(f, "read failure"))
    }
  }

  private def transformAndThen(lastCheckpoint: C, inDelta: Delta, inData: I): Unit = {
    Try(transformer.execute(inData)) match {
      case Success(outData) =>
        writeAndThen(lastCheckpoint, inDelta, outData)
      case Failure(f) => statusUpdate(new StatusError(f, "transform failure"))
    }
  }

  private def writeAndThen(lastCheckpoint: C, inDelta: Delta, outData: O): Unit = {
    Try(writer.execute(outData)) match {
      case Success(outDelta) =>
        statusUpdate(new StatusOK("ready to checkpoint"))
        commitCheckpoint(
          lastCheckpoint
            .mergeDelta(outDelta.getOrElse(inDelta)))
      case Failure(f)=>
        statusUpdate(new StatusError(f, "write failure"))
    }
  }

  private def commitCheckpoint(cp: C): Unit = {
    Try(checkpointService.executeCommit(cp)) match {
      case Success(_) =>
        statusUpdate(new StatusOK(s"checkpoint success ${cp}"))
      case Failure(f) =>
        statusUpdate(new StatusError(f, "checkpoint commit failure"))
    }
  }

  def runOneCycle(): Unit = {
    cycleStarted()
    loadCheckpointAndThen()
    cycleCompleted()
  }

  def start(pollingInterVal: Long = DefaultPollingInterval): Unit = {
    Try(reader.start())
    match {
      case Success(_) =>
        Try(writer.start())
        match {
          case Success(_) =>
            scala.sys.addShutdownHook(close)
            while (true) {
              runOneCycle()
              Thread.sleep(pollingInterVal)
            }
          case Failure(f) => throw new Exception(s"Failed to start writer ${writer}", f)
        }
      case Failure(f) => throw new Exception(s"Failed to start reader ${reader}", f)
    }

  }

  def close(): Unit = {
    statusUpdate(new StatusOK("Shutting down"))
    reader.close()
    writer.close()
  }
}
