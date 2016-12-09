package com.creditkarma.blink.factory

import scala.util.{Failure, Success, Try}

/**
  * Used by the portal caster configuration system
  * It has a strict policy to check duplicates in order to prevent potential mis-configurations
  */
trait SettableProperties extends PropertySetter with PropertyGetter {
  private val properties: collection.mutable.Map[String, String] = collection.mutable.Map.empty

  override def set(name: String, value: String): Unit = {
    properties.get(name) match {
      case Some(existingValue) =>
        throw new Exception(s"$name=$existingValue already exists, cannot be reset to $value")
      case None => properties(name) = value
    }
  }

  override def get(name: String): String = {
    properties.get(name) match {
      case Some(value) => value
      case None => throw new Exception(s"$name is not set")
    }
  }

  override def getLong(name: String): Long = {
    Try(get(name)) match {
      case Success(strValue) =>
        Try(strValue.toLong) match {
          case Success(intervalLong) => intervalLong
          case Failure(f) => throw new Exception(s"$name=$strValue cannot be converted to Long", f)
        }
      case Failure(f) => throw f
    }
  }

  override def getOption(name: String): Option[String] = {
    properties.get(name)
  }

  override def getPropertiesByPrefix(prefix: String): Map[String, String] = {
    properties.filter(_._1.startsWith(prefix)).toMap
  }

  override def allProperties: Map[String, String] = properties.toMap
}

trait PropertySetter {
  def set(name: String, value: String): Unit
}

trait PropertyGetter {
  def get(name: String): String
  def getOption(name: String): Option[String]
  def getLong(name: String): Long = throw new Exception("not supported")
  def getPropertiesByPrefix(prefix: String): Map[String, String]
  def allProperties: Map[String, String]
}
