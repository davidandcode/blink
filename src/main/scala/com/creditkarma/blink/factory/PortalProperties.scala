package com.creditkarma.blink.factory

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

/**
  * The properties should contain globally accessible properties by all the modules
  * The module should properly manage their own namespace by using lower case dot connected strings for the property name
  */
trait PortalProperties extends PropertyGetter {
  def loadProperties(file: String): PortalProperties
}

object PortalProperties {
  val properties = new Properties()
  def apply(): PortalProperties = new PortalProperties {

    override def getPropertiesByPrefix(prefix: String): Map[String, String] = {
      {for(
        name <- properties.stringPropertyNames().asScala.filter(_.startsWith(prefix));
        value = properties.getProperty(name)
      )yield{
        name -> value
      }}.toMap
    }

    override def loadProperties(file: String): PortalProperties = {
      properties.load(new FileInputStream(new File(file)))
      this
    }

    // the getter interface is nicer then using option
    override def getOrFail(name: String): String = {
      get(name) match {
        case Some(value) => value
        case None => throw new Exception(s"$name is not set")
      }
    }

    override def allProperties: Map[String, String] = getPropertiesByPrefix("")

    // not supported on this class
    override def get(name: String): Option[String] = Option(properties.getProperty(name))
  }
}
