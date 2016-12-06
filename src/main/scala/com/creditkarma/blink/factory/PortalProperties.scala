package com.creditkarma.blink.factory

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

/**
  * The properties should contain globally accessible properties by all the modules
  * The module should peroperly manage their own namespace by using lower case dot connected strings for the property name
  */
trait PortalProperties {
  def loadProperties(file: String): PortalProperties

  def getProperty(key: String): Option[String]

  // this interface can used to retrieve all spark properties and set it through SparkConf
  // Or kafka related properties sent to Kafka: consumer, or producer etc
  def getPropertiesByPrefix(prefix: String): Map[String, String]
}

object PortalProperties {
  val properties = new Properties()
  def apply(): PortalProperties = new PortalProperties {
    override def getProperty(key: String): Option[String] = {
      Option(properties.getProperty(key))
    }

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
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    /*val properties = new Properties()
    properties.load(new FileInputStream(new File("config/kafka.test.properties")))
    println(properties.stringPropertyNames())
    println(properties.propertyNames().nextElement())*/
    val prop = PortalProperties().loadProperties("config/kafka.test.properties")
    prop.getPropertiesByPrefix("").foreach(println)
  }
}