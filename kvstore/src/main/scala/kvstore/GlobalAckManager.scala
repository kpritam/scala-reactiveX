package kvstore

import akka.actor.{Actor, ActorRef, Props, Scheduler, Timers}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.PersistenceManager.PersistSuccess
import kvstore.Replica.UpdateRequest
import kvstore.ReplicationManager.ReplicationSuccess
import kvstore.Replicator.Replicated

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

object GlobalAckManager {

  case object Stop

  case class GlobalAck(key: String, id: Long, replyTo: ActorRef, success: Boolean)

  def props(updateRequest: UpdateRequest, replyTo: ActorRef, persistenceActor: ActorRef, totalReplicators: Int) =
    Props(new GlobalAckManager(updateRequest, replyTo, persistenceActor, totalReplicators))
}

class GlobalAckManager(updateRequest: UpdateRequest, replyTo: ActorRef, persistenceActor: ActorRef, totalReplicators: Int)
  extends Actor with Timers {

  import GlobalAckManager._
  import updateRequest._

  val timerKey = s"$key-$id"

  private val persistenceMgr: ActorRef = context.actorOf(PersistenceManager.props(updateRequest, persistenceActor))
  private lazy val replicationMgr: ActorRef = context.actorOf(ReplicationManager.props(key, id, totalReplicators))

  val scheduler: Scheduler = context.system.scheduler
  implicit val ec: ExecutionContextExecutor = context.dispatcher
  var persistSuccessful = false
  var replicationSuccessful: Boolean = totalReplicators == 0

  override def preStart(): Unit = timers.startSingleTimer(timerKey, Stop, 1.seconds)

  override def receive: Receive = {
    case persist@Persist ⇒ persistenceMgr ! persist
    case persisted: Persisted ⇒ persistenceMgr ! persisted
    case replicated: Replicated ⇒ replicationMgr ! replicated
    case PersistSuccess(`key`, `id`) ⇒ if (replicationSuccessful || onlyPersist) sendPRFinished(true) else persistSuccessful = true
    case ReplicationSuccess(`key`, `id`) ⇒ if (persistSuccessful) sendPRFinished(true) else replicationSuccessful = true
    case Stop ⇒ sendPRFinished(false)
  }

  private def sendPRFinished(globalAck: Boolean): Unit = {
    context.parent ! GlobalAck(key, id, replyTo, globalAck)
    stop()
  }

  private def stop(): Unit = {
    timers.cancel(timerKey)
    context.stop(self)
  }

}
