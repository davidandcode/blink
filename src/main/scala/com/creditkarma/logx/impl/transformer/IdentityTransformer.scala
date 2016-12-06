package com.creditkarma.logx.impl.transformer

import com.creditkarma.logx.base.{BufferedData, Transformer}

class IdentityTransformer[I <: BufferedData] extends Transformer[I, I]{
  override def transform(input: I): I = input
}
