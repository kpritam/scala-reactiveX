package kvstore

import akka.actor.{Actor, ActorRef, Props}
import kvstore.Replicator.Replicated

object ReplicationTracker {

  case class ReplicationFinished(key: String, id: Long)

  def props(key: String, id: Long, replicators: Set[ActorRef]) =
    Props(new ReplicationTracker(key, id, replicators))
}

class ReplicationTracker(key: String, id: Long, replicators: Set[ActorRef]) extends Actor {

  import ReplicationTracker._

  var replicationCompleted: Long = 0

  override def receive: Receive = {
    case Replicated(`key`, `id`) ⇒
      replicationCompleted += 1
      if (replicationCompleted == replicators.size) {
        context.parent ! ReplicationFinished(key, id)
        context.stop(self)
      }
  }

}
