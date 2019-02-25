package kvstore

import akka.actor.{Actor, ActorRef, Props, Scheduler, Timers}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.PersistenceManager.PersistSuccess
import kvstore.Replica.{ReplicatorsTerminated, UpdateRequest}
import kvstore.ReplicationManager.ReplicationSuccess
import kvstore.Replicator.Replicated

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

object GlobalAckManager {

  case object Stop

  case class GlobalAck(key: String, id: Long, replyTo: ActorRef, success: Boolean)

  def props(updateRequest: UpdateRequest, replyTo: ActorRef, persistenceActor: ActorRef, replicators: Set[ActorRef]) =
    Props(new GlobalAckManager(updateRequest, replyTo, persistenceActor, replicators))
}

class GlobalAckManager(updateRequest: UpdateRequest, replyTo: ActorRef, persistenceActor: ActorRef, replicators: Set[ActorRef])
  extends Actor with Timers {

  import GlobalAckManager._
  import updateRequest._

  val timerKey = s"$key-$id"

  private val persistenceMgr: ActorRef = context.actorOf(PersistenceManager.props(updateRequest, persistenceActor))
  private lazy val replicationMgr: ActorRef = context.actorOf(ReplicationManager.props(key, id, replicators))

  val scheduler: Scheduler = context.system.scheduler
  implicit val ec: ExecutionContextExecutor = context.dispatcher
  var persistSuccessful = false
  var replicationSuccessful: Boolean = replicators.isEmpty

  override def preStart(): Unit = timers.startSingleTimer(timerKey, Stop, 1.seconds)

  override def receive: Receive = {
    case persist@Persist ⇒ persistenceMgr ! persist
    case persisted: Persisted ⇒ persistenceMgr ! persisted
    case replicated: Replicated ⇒ replicationMgr forward replicated
    case PersistSuccess(`key`, `id`) ⇒ if (replicationSuccessful || onlyPersist) sendGlobalAck(true) else persistSuccessful = true
    case ReplicationSuccess(`key`, `id`) ⇒ if (persistSuccessful) sendGlobalAck(true) else replicationSuccessful = true
    case replicatorsTerminated: ReplicatorsTerminated ⇒ replicationMgr ! replicatorsTerminated
    case Stop ⇒ sendGlobalAck(false)
  }

  private def sendGlobalAck(globalAck: Boolean): Unit = {
    context.parent ! GlobalAck(key, id, replyTo, globalAck)
    stop()
  }

  private def stop(): Unit = {
    timers.cancel(timerKey)
    context.stop(self)
  }

}
