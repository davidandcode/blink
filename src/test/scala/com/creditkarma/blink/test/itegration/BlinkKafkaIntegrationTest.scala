package com.creditkarma.blink.test.itegration

import com.creditkarma.blink.base.{OperationMode, TimeMode}
import com.creditkarma.blink.instrumentation.LogInfoInstrumentor
/**
  * Kafka Integration Test trait with pluggable modules
  * This version only support plugging checkpoint service and the single threaded writer, which are the main focus of testing right now.
  * In the future, a more flexible testing trait can be used to plugin very thing including source data test data generator.
  */
trait BlinkKafkaIntegrationTest extends KafkaIntegrationTestBase {

  feature("Very simple basic test") {

    scenario("Kafka should receive all messages") {

      Given("2 topics created")
      createTopic("test1", 2)
      createTopic("test2", 3)

      And("5 messages sent to each topic")
      sendMessage("test1", "k1", "v1")
      sendMessage("test1", "k2", "v2")
      sendMessage("test1", "k3", "v3")
      sendMessage("test1", "k4", "v4")
      sendMessage("test1", "k5", "v5")

      sendMessage("test2", "k1", "v1")
      sendMessage("test2", "k2", "v2")
      sendMessage("test2", "k3", "v3")
      sendMessage("test2", "k4", "v4")
      sendMessage("test2", "k5", "v5")

      Then("there should be 10 messages in total")
      assert(allMessages.size == 10)
    }

    scenario("Send all data to a single portal with one flush") {

      Given("a test-portal which will flush all messages in one cycle")
      val portalId = "test-portal"
      val portal = getOrCreatePortal(portalId, flushSize = 50)

      And("the log instrumentor is registered to show the trace, but otherwise doesn't matter")
      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace

      When("running the portal until no data is pushed")
      //portal.runTilCompletion()
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

      Then("the writer threads should collectively observe the same data")
      assert(getWriter.collect.get(portalId).get.sortBy(_.toString) == allMessages.sortBy(_.toString))
    }

    scenario("Two portals from Kafka with independent checkpoint") {
      Given("2 portals each flush 1 message per time per partition")
      val id1 = "portal_1of2"
      val id2 = "portal_2of2"
      val portal1 = getOrCreatePortal(id1, flushSize = 1)
      val portal2 = getOrCreatePortal(id2, flushSize = 1)

      And("the log instrumentor is registered to show the trace, but otherwise doesn't matter")
      portal1.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      portal2.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace

      portal1.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)
      portal2.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)

      Then("the writer threads should collectively observe the same data for each portal")
      assert(getWriter.collect.get(id1).get.map(_.toString).sorted == allMessages.map(_.toString).sorted)
      assert(getWriter.collect.get(id2).get.map(_.toString).sorted  == allMessages.map(_.toString).sorted )
    }

    scenario("Portal must be able to reset position") {
      Given("a new portal")
      val id = "new-portal"
      val portal = getOrCreatePortal(id, flushSize = 1)
      And("the log instrumentor is registered to show the trace, but otherwise doesn't matter")
      portal.registerInstrumentor(LogInfoInstrumentor()) // this is just to observe trace
      When("starting from now")
      And("running the portal until completion")
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Now)
      Then("nothing should have been written")
      assert(getWriter.collect.getOrElse(id, Seq.empty).isEmpty)

      When("starting from earliest")
      And("running the portal until completion")
      portal.openPortal(OperationMode.ImporterDepletion, TimeMode.Origin)
      Then("the writer threads should collectively observe the same data")
      assert(getWriter.collect.get(id).get.map(_.toString).sorted == allMessages.map(_.toString).sorted)
    }
  }
}