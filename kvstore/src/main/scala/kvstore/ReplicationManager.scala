package kvstore

import akka.actor.{Actor, ActorRef, Props, Terminated}
import kvstore.Replica.ReplicatorsTerminated
import kvstore.Replicator.Replicated

object ReplicationManager {

  case class Replicators(replicators: Set[ActorRef])

  case class ReplicationSuccess(key: String, id: Long)

  def props(key: String, id: Long, replicators: Set[ActorRef]) =
    Props(new ReplicationManager(key, id, replicators))
}

class ReplicationManager(val key: String, val id: Long, replicators: Set[ActorRef]) extends Actor {

  import ReplicationManager._

  var currentReplicators: Set[ActorRef] = replicators
  var acks = Set.empty[ActorRef]

  override def preStart(): Unit = replicators.foreach(context.watch)

  override def receive: Receive = {
    case Replicators(replicators) ⇒
      currentReplicators = replicators
      replicators.foreach(context.watch)
      acks = acks intersect currentReplicators
    case Replicated(`key`, `id`) ⇒
      acks = acks + sender()
      checkForReplicationCompletion()
    case ReplicatorsTerminated(replicators) ⇒
      currentReplicators = currentReplicators -- replicators
      checkForReplicationCompletion()

    case Terminated(ref) ⇒ currentReplicators = currentReplicators - ref
  }

  private def checkForReplicationCompletion(): Unit =
    if ((currentReplicators diff acks).isEmpty) {
      context.parent ! ReplicationSuccess(key, id)
      context.stop(self)
    }


}
