package com.creditkarma.blink.base

/**
  *
  * @tparam D delta of the checkpoint
  * @tparam C checkpoint self type
  */
trait Checkpoint[D, C <: Checkpoint[D, C]] extends Serializable {
  /**
    * Checkpoint should support merging operation to get new checkpoint by merging with delta
    * @param delta
    * @return
    */
  def mergeDelta(delta: D, inTime: Long): C
  type DeltaType = D
}
