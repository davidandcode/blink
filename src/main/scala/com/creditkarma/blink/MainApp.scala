package com.creditkarma.blink

import com.creditkarma.blink.base.{OperationMode, TimeMode}
import com.creditkarma.blink.factory.{KafkaStringPartitionWriterCreator, PortalFactory, PortalProperties}
import com.creditkarma.blink.impl.transformer.KafkaMessageWithId
import com.creditkarma.blink.impl.writer.{KafkaPartitionWriter, WriterClientMeta}
import com.creditkarma.blink.utils.LazyLog
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yongjia.wang on 12/2/16.
  */
object MainApp extends LazyLog{

  object KW {
    val PORTAL_ID_PARAM = "blink.portal.id"
    val PORTAL_FACTORY_CLASS = "blink.portal.factory.class"
    val PORTAL_FACTORY_PARAM = "blink.portal.factory.properties"
    val BLINK_ENGINE_PARAM = "blink.engine"
  }

  def main(args: Array[String]): Unit = {
    if(args.size == 0){
      throw new Exception(s"Blink must take a config file as arg")
    }
    else{
      castPortal(args(0))
    }
  }

  def castPortal(configFile: String): String = {
    val properties = PortalProperties().loadProperties(configFile)
    def get(name: String) = properties.get(name)

    /**
      * First initialize the engine
      * there should be only a few options, just enumerate them instead of using DI pattern
      */
    get(KW.BLINK_ENGINE_PARAM) match {
      case "spark" =>
        info(s"Creating spark context")
        val sparkConf = new SparkConf()
        for (
          (key, value) <- properties.getPropertiesByPrefix("spark.")
        ) {
          sparkConf.set(key, value)
        }
        SparkContext.getOrCreate(sparkConf)

      case otherEngine => throw new Exception(s"${KW.BLINK_ENGINE_PARAM}=${otherEngine} not supported")
    }

    /**
      * Then build the portal
      * using dependency injection with dynamic class loading
      */
    val factoryClassName = get(KW.PORTAL_FACTORY_CLASS)
    // Find the portal factory class and create an instance
    val portalFactory = Class.forName(factoryClassName).newInstance().asInstanceOf[PortalFactory]
    // set up its properties as required by the dependency injection
    for (
      (key, value) <- properties.getPropertiesByPrefix(KW.PORTAL_FACTORY_PARAM + ".");
      factoryPropertyName = key.substring(KW.PORTAL_FACTORY_PARAM.length + 1)
    ) {
      portalFactory.set(factoryPropertyName, value)
    }
    // build the portal controller
    val portalController = portalFactory.build()

    /**
      * Finally open the portal and start warping
      */
    //TODO figure out the portal operation mode from configuration
    portalController.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

    portalController.portalId
  }
}