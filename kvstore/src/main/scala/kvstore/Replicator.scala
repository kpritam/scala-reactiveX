package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}

import scala.concurrent.duration._

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  type PrimaryReplica = ActorRef

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (PrimaryReplica, Replicate, Cancellable)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  private def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case replicate@Replicate(key, value, _) ⇒
      val seq = nextSeq()
      val snapshot = Snapshot(key, value, seq)
      replica ! snapshot
      val cancellable = context.system.scheduler.schedule(100.millis, 100.millis, replica, snapshot)
      acks = acks.updated(seq, (sender(), replicate, cancellable))
    case SnapshotAck(key, seq) ⇒
      acks.get(seq).foreach {
        case (ref, replicate, cancellable) ⇒ cancellable.cancel(); ref ! Replicated(key, replicate.id)
      }
  }

}
