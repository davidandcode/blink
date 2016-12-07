package com.creditkarma.blink.factory

import com.creditkarma.blink.base.PortalController

/**
  * In order to cast a portal from properties file based configuration, a [[PortalFactory]] class with a zero-argument constructor must be implemented.
  * [[com.creditkarma.blink.MainApp]] will instantiate the factory class from the properties file using basic java reflection.
  * The factory class instance will receive all properties based on prefix of the property name, and it then can decide how to create the portal using the properties.
  * The design is very flexible in the sense that the factory class itself has full control of the entire creation process.
  * It can use its own factory/reflection based strategies to create a wider class of portals. And it can decide how to detect and handle configuration errors, etc.
  * With the chain of resposibility, the create mechanism provides effectively unlimited extendability.
  * Only need to follow the basic convention of the properties file format, in order to provide the minimum requirement for property namespace management.
  */
trait PortalFactory extends SettableProperties {
  def build(): PortalController
}