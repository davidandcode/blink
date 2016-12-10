package com.creditkarma.blink.impl.spark.importer.kafka

import kafka.consumer.{Blacklist, TopicFilter, Whitelist}
import org.apache.kafka.common.internals.TopicConstants

/**
  * It may be arguable how to use the combination of whitelist and blacklist with regex matching.
  * By convention, whitelist has higher priority than blacklist: if a whitelist's regex matches, the blacklist doesn't matter.
  * In reality, the whitelist needs to have different semantics under different context:
  * 1. When there is the whitelist only, it should perform both the allow and disallow function, simply based on regex matching outcome.
  * 2. When a black list exists, the whitelist should only perform the allow function, overriding the decision of blacklist,
  * and let everything else pass on to the blacklist, instead of immediately disallow it due to regex mismatch as in the previous case.
  * This may sound inconsistent from certain perspective, but the following 3x3 truth table shows the logic is symmetric.
  * ┌─────────────────┬────────────────────────┐
  * │                 │  Whitelist allowed     │
  * │                 ├───────┬───────┬────────┤
  * │                 │  Yes  │  No   │ Empty  │
  * ├─────────┬───────┼───────┼───────┼────────┤
  * │         │  Yes  │  Yes  │  Yes  │  Yes   │
  * │         ├───────┼───────┼───────┼────────┤
  * │Blacklist│  No   │  Yes  │  No   │   No   │
  * │ allowed ├───────┼───────┼───────┼────────┤
  * │         │ Empty │  Yes  │  No   │Internal│
  * └─────────┴───────┴───────┴───────┴────────┘
  * @param whitelist
  * @param blacklist
  * @param excludeInternalTopics
  */
class KafkaTopicFilter(whitelist: Option[Whitelist], blacklist: Option[Blacklist], excludeInternalTopics: Boolean = true){
  def isAllowed(topic: String): Boolean = {
    def matchedFilter(filter: TopicFilter): Boolean = filter.isTopicAllowed(topic, excludeInternalTopics)

    // don't like copying the code from kafka.consumer.TopicFilter, but if we allow both whitelist and blacklist to be empty
    // which is considered matching for any topic, then we have to test internal topic filter
    def passedInternalTopicCheck: Boolean = !(TopicConstants.INTERNAL_TOPICS.contains(topic) && excludeInternalTopics)

    whitelist.exists(matchedFilter) || // if whitelist is non-empty and matched filter, topic is allowed regardless of blacklist
      blacklist.exists(matchedFilter) || // otherwise blacklist, it has to also pass internal topic filter
        whitelist.isEmpty && blacklist.isEmpty && passedInternalTopicCheck
    // at this point, it's possible both blacklist and whitelist are empty, in that case check internal topic
    // if either of them is non-empty, internal topic filter must have already failed
  }
  override def toString: String = s"$whitelist, $blacklist, excludeInternalTopics=$excludeInternalTopics"
}
