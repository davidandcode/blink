package com.creditkarma.logx.factory

/**
  * Used by the portal caster configuration system
  * It has a strict policy to check duplicates in order to prevent potential mis-configurations
  */
trait SettableProperties {
  private val properties: collection.mutable.Map[String, String] = collection.mutable.Map.empty
  def set(name: String, value: String): Unit = {
    properties.get(name) match {
      case Some(existingValue) =>
        throw new Exception(s"$name=$existingValue already exists, cannot be reset to $value")
      case None => properties(name) = value
    }
  }
  def get(name: String): Option[String] = properties.get(name)
}
