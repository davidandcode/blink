package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */

final class LogXCore[I <: BufferedData, O <: BufferedData, C <: Checkpoint[Delta, C], Delta]
(
  val appName: String,
  reader: Reader[I, C, Delta, _],
  transformer: Transformer[I, O],
  writer: Writer[O, C, Delta, _],
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
  
  private val DefaultTickTime = 1000L
  var tickTime: Long = DefaultTickTime

  /**
    *
    * @return whether any data is flushed in this cycle,
    *         if true, it's likely there are more data ready to be pushed, although there can be quite a few scenarios
    *         A simple solution is to immediately run anothe cycle without waiting for the polling interval.
    */
  private def loadCheckpointAndThen(): Boolean = {
    Try(checkpointService.executeLoad()) match {
      case Success(lastCheckpoint) =>
        updateStatus(new StatusOK(s"got last checkpoint ${lastCheckpoint}"))
        readAndThen(lastCheckpoint)
      case Failure(f) =>
        updateStatus(new StatusError(f, "checkpoint load failure"))
        false
    }
  }

  private def readAndThen(lastCheckpoint: C): Boolean = {
    Try(reader.execute(lastCheckpoint)) match {
      case Success((inData, inDelta, flush, inTime)) =>
        if(flush) {
          updateStatus(new StatusOK("ready to flush"))
          transformAndThen(lastCheckpoint, inTime, inDelta, inData)
        }
        else{
          updateStatus(new StatusOK("not enough to flush"))
          false
        }
      case Failure(f) =>
        updateStatus(new StatusError(f, "read failure"))
        false
    }
  }

  private def transformAndThen(lastCheckpoint: C, inTime: Long, inDelta: Delta, inData: I): Boolean = {
    Try(transformer.execute(inData)) match {
      case Success(outData) =>
        writeAndThen(lastCheckpoint, inTime, inDelta, outData)
      case Failure(f) =>
        updateStatus(new StatusError(f, "transform failure"))
        false
    }
  }

  private def writeAndThen(lastCheckpoint: C, inTime: Long, inDelta: Delta, outData: O): Boolean = {
    Try(writer.execute(outData, lastCheckpoint, inTime, inDelta)) match {
      case Success((outDelta, outRecords)) =>
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
        //throw f
        false
    }
  }

  private def commitCheckpoint(cp: C): Unit = {
    Try(checkpointService.executeCommit(cp)) match {
      case Success(_) =>
        updateStatus(new StatusOK(s"checkpoint success ${cp}"))
      case Failure(f) =>
        updateStatus(new StatusError(f, "checkpoint commit failure"))
    }
  }

  def runOneCycle(): Boolean = {
    cycleStarted()
    val dataFlushed = loadCheckpointAndThen()
    cycleCompleted()
    dataFlushed
  }

  /**
    * Stay in an infinite loop
    */
  def start(): Unit = {
    Try(reader.start()) match {
      case Success(_) =>
        Try(writer.start()) match {
          case Success(_) =>
            scala.sys.addShutdownHook(close)
            while (true) {
              while(runOneCycle()){
                updateStatus(new StatusOK(s"run cycle immediately after data push"))
              }
              updateStatus(new StatusOK(s"No data pushed, wait for ${tickTime} ms"))
              Thread.sleep(tickTime)
            }
          case Failure(f) => throw new Exception(s"Failed to start writer ${writer}", f)
        }
      case Failure(f) => throw new Exception(s"Failed to start reader ${reader}", f)
    }
  }

  def close(): Unit = {
    updateStatus(new StatusOK("Shutting down"))
    reader.close()
    writer.close()
  }
}
