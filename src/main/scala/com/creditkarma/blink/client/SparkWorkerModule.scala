package com.creditkarma.blink.client

import com.creditkarma.blink.base.ModuleType._
import com.creditkarma.blink.base.StatusOK

/**
  * All the client modules must be serializable
  */
trait SparkWorkerModule extends Serializable {
  def moduleType: ClientModuleType.Value

  def portalId: String = {
    _portalId match{
      case Some(id) => id
      case None => throw new Exception(s"${moduleType} portal id is not set")
    }
  }
  private var _portalId: Option[String] = None

  def registerPortal(id: String): Unit = {
    _portalId match{
      case Some(otherId) =>
        if(otherId != id){
          throw new Exception(s"${moduleType} already registered with another portal ${otherId} != ${id}")
        }
      case None => _portalId = Some(id)
    }
  }
}

object ClientModuleType extends Enumeration {
  val SingleThreadWriter = Value
}
