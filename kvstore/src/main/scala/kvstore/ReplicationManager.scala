package kvstore

import akka.actor.{Actor, Props}
import kvstore.Replicator.Replicated

object ReplicationManager {

  case class ReplicationSuccess(key: String, id: Long)

  def props(key: String, id: Long, totalReplicators: Int) =
    Props(new ReplicationManager(key, id, totalReplicators))
}

class ReplicationManager(val key: String, val id: Long, totalReplicators: Int) extends Actor {

  import ReplicationManager._

  var replicationCompleted: Long = 0

  override def receive: Receive = {
    case Replicated(`key`, `id`) ⇒
      replicationCompleted += 1
      if (replicationCompleted == totalReplicators) {
        context.parent ! ReplicationSuccess(key, id)
        context.stop(self)
      }
  }

}
