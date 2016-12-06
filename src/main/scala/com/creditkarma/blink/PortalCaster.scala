package com.creditkarma.blink

import com.creditkarma.blink.factory.{PortalFactory, PortalProperties}

/**
  * Created by yongjia.wang on 12/2/16.
  */
object PortalCaster {
  object KW {
    val PORTAL_FACTORY_CLASS = "blink.portal.factory.class"
    val PORTAL_FACTORY_PARAM = "blink.portal.factory.properties"
  }

  def main(args: Array[String]): Unit = {
    castAndRunPortal("config/kafka.test.properties")
  }

  def castAndRunPortal(configFile: String): Unit = {
    val properties = PortalProperties().loadProperties(configFile)
    properties.getProperty(KW.PORTAL_FACTORY_CLASS) match {
      case Some(factoryClassName) =>
        val portalFactory = Class.forName(factoryClassName).newInstance().asInstanceOf[PortalFactory]
        for(
          (key, value) <- properties.getPropertiesByPrefix(KW.PORTAL_FACTORY_PARAM);
          factoryPropertyName = key.substring(KW.PORTAL_FACTORY_PARAM.length + 1)
        ){
          portalFactory.set(factoryPropertyName, value)
        }
        portalFactory.build()
      case None => throw new Exception(s"${KW.PORTAL_FACTORY_CLASS} not found in configuration: ${properties.getPropertiesByPrefix("")}")
    }

  }
}
