package com.creditkarma.logx.client

import com.creditkarma.logx.base.ModuleType._

/**
  * All the client modules must be serializable
  */
trait ClientModule extends Serializable {
  def moduleType: ClientModuleType.Value
  var _portalId: Option[String] = None
  def portalId: String = {
    _portalId match{
      case Some(id) => id
      case None => throw new Exception(s"${moduleType} portal id is not set")
    }
  }
}

object ClientModuleType extends Enumeration {
  val SingleThreadWriter = Value
}
