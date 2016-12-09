package com.creditkarma.blink.impl.spark.exporter.kafka

/**
  * Created by yongjia.wang on 12/7/16.
  */
case class KafkaSubPartitionMeta[P](partitionInfo: KafkaSubPartition[P], clientMeta: WriterClientMeta)
