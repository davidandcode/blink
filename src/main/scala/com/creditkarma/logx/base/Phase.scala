package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/22/16.
  */
object Phase extends Enumeration{
  val
  CheckpointLoad, Read, Transform, Write, CheckpointCommit
  = Value
}
