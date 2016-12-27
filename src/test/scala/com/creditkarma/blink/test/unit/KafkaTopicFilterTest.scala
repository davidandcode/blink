package com.creditkarma.blink.test.unit

import com.creditkarma.blink.impl.spark.importer.kafka.KafkaTopicFilter
import kafka.consumer.{Blacklist, Whitelist}
import org.scalatest.WordSpec

class KafkaTopicFilterTest extends WordSpec {

  "An empty filter" must {
    "not allow internal topics" in {
      val filter = new KafkaTopicFilter(None, None)
      assert(!filter.isAllowed("__consumer_offsets"))
    }

    "unless explicitly told so" in {
      val filter = new KafkaTopicFilter(None, None, false)
      assert(filter.isAllowed("__consumer_offsets"))
    }
  }

  "A whitelist only filter" must {
    val filter = new KafkaTopicFilter(Some(Whitelist(".*topic$")), None)

    "allow topic when match" in {
      assert(filter.isAllowed("any_topic"))
    }

    "not allow topic when not match" in {
      assert(!filter.isAllowed("any_topic1"))
    }

    "still not allow internal topics" in {
      assert(!filter.isAllowed("__consumer_offsets"))
    }

    "A blacklist only filter" must {
      val filter = new KafkaTopicFilter(None, Some(Blacklist(".*blacklisted")))

      "allow topics as long as it's not internal topic" in {
        assert(filter.isAllowed("any"))
        assert(!filter.isAllowed("__consumer_offsets"))
      }

      "not allow topics when match" in {
        assert(!filter.isAllowed("not_blacklisted"))
      }
    }

    "A filter with both whitelist and blacklist" must {
      val filter = new KafkaTopicFilter(Some(Whitelist(".*not_blacklisted")), Some(Blacklist(".*blacklisted")))

      "allow topic as long whitelist matches, even blacklist also matches" in {
        assert(filter.isAllowed("not_blacklisted"))
      }

      "otherwise check blacklist" in {
        assert(!filter.isAllowed("blacklisted"))
      }

      // this behavior is different from the whitelist only scenairo where the pattern must match
      "but allow topics not matching to either" in {
        assert(filter.isAllowed("random topic"))
      }

      "still not allow internal topics" in {
        assert(!filter.isAllowed("__consumer_offsets"))
      }
    }
  }
}
