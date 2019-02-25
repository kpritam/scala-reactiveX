/**
  * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
  */
package kvstore

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import kvstore.Replica._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar.convertFloatToGrainOfTime
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.Await

trait IntegrationSpec
  extends FunSuiteLike
    with Matchers with Eventually {
  this: KVStoreSuite =>

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  implicit val timeout: Timeout = Timeout(5.seconds)

  private def createPrimary(arbiter: ActorRef) = {
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(false)), "primary")
    // wait for primary to become leader
    Thread.sleep(1000)
    primary
  }

  private def createSecondaries(n: Int, arbiter: ActorRef) =
    (1 to n).map { id ⇒
      system.actorOf(Replica.props(arbiter, Persistence.props(false)), s"secondary-$id")
    }

  private def sendAndExpect(ref: ActorRef, msg: Any, expect: Any) =
    Await.result(ref ? msg, 5.seconds) shouldBe expect

  private def stop(refs: Seq[ActorRef]): Unit = refs.foreach(system.stop)

  test("primary should support Insert, Remove & Get, secondaries should support Get") {
    val arbiter: ActorRef = system.actorOf(Props(new Arbiter), "arbiter")

    val primary = createPrimary(arbiter)
    val secondaries = createSecondaries(10, arbiter)

    sendAndExpect(primary, Insert("k-1", "1", 1), OperationAck(1))
    sendAndExpect(primary, Get("k-1", 2), GetResult("k-1", Some("1"), 2))
    secondaries.zipWithIndex.foreach { case (secondary, id) ⇒
      sendAndExpect(secondary, Get("k-1", id), GetResult("k-1", Some("1"), id))
    }

    sendAndExpect(primary, Remove("k-1", 10), OperationAck(10))
    sendAndExpect(primary, Get("k-1", 11), GetResult("k-1", None, 11))
    secondaries.zipWithIndex.foreach { case (secondary, id) ⇒
      sendAndExpect(secondary, Get("k-1", id), GetResult("k-1", None, id))
    }

    stop(secondaries :+ arbiter :+ primary)
  }

  test("lossy: primary should support Insert, Remove & Get, secondaries should support Get") {
    val probe = TestProbe()
    val arbiter: ActorRef = system.actorOf(Props(new given.Arbiter(true, probe.ref)), "arbiter")

    val primary = createPrimary(arbiter)
    val secondaries = createSecondaries(10, arbiter)

    sendAndExpect(primary, Insert("k-1", "1", 1), OperationAck(1))
    sendAndExpect(primary, Get("k-1", 2), GetResult("k-1", Some("1"), 2))
    secondaries.zipWithIndex.foreach { case (secondary, id) ⇒
      sendAndExpect(secondary, Get("k-1", id), GetResult("k-1", Some("1"), id))
    }

    sendAndExpect(primary, Remove("k-1", 10), OperationAck(10))
    sendAndExpect(primary, Get("k-1", 11), GetResult("k-1", None, 11))
    secondaries.zipWithIndex.foreach { case (secondary, id) ⇒
      sendAndExpect(secondary, Get("k-1", id), GetResult("k-1", None, id))
    }

    stop(secondaries :+ arbiter :+ primary)
  }

}
