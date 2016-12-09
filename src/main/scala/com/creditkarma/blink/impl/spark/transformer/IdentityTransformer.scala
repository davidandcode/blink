package com.creditkarma.blink.impl.spark.transformer

import com.creditkarma.blink.base.{BufferedData, Transformer}

class IdentityTransformer[I <: BufferedData] extends Transformer[I, I]{
  override def transform(input: I): I = input
}
