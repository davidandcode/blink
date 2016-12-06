package com.creditkarma.blink.base

/**
  * Dimensions are descriptions of the [[Metric]] and used to slice and dice
  * Fields are numeric values of the [[Metric]]
  * This general structure appear in a wide range of analytics frameworks although with different terminologies:
  * Tableau separates columns into 'dimensions' (for slice and dice) and 'metrics' (for aggregation)
  * In Tableau, a numeric column can be converted between dimension and metric, but categorical column can only be a dimension.
  * InfluxDB has 2 types of columns: Tag (same as dimension) and Field, where Tag is indexed by the system for faster slice and dice.
  * This design of [[Metric]] can be easily mapped to influxDB protocol and takes full advantage of it.
  */
trait Metric {
  def dimensions: Map[Any, Any]
  def fields: Map[Any, Any]
}

trait Metrics {
  def metrics: Iterable[Metric]
}
