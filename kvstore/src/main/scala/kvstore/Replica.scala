package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, Scheduler, Timers}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.GlobalAckManager.GlobalAck
import kvstore.Persistence.Persist

import scala.concurrent.duration._
import scala.util.Random

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class UpdateRequest(key: String, value: Option[String], id: Long, onlyPersist: Boolean = false)

  case class ReplicaTerminated(replica: ActorRef)

  case class ReplicatorsTerminated(replicators: Set[ActorRef])

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers {

  import Replica._
  import Replicator._

  type SecondaryReplica = ActorRef
  type Replicator = ActorRef

  var kv = Map.empty[String, String]
  val replicaStore = new ReplicaStore(context)
  //  import replicaStore._

  var lastSnapshotSeq: Long = -1L

  val persistenceActor: ActorRef = context.actorOf(persistenceProps, "persistence-actor")
  val scheduler: Scheduler = context.system.scheduler
  var cancellables = Map.empty[String, (ActorRef, Cancellable)]
  var globalAcks = Map.empty[(String, Long), ActorRef]

  implicit val timeout: Timeout = Timeout(1.seconds)

  override def preStart(): Unit = arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary ⇒ context.become(leader)
    case JoinedSecondary ⇒ context.become(replica)
  }

  def createGlobalAckManager(updateRequest: UpdateRequest): ActorRef = {
    val ref = context.actorOf(GlobalAckManager.props(updateRequest, sender(), persistenceActor, replicaStore.replicators))
    globalAcks = globalAcks.updated((updateRequest.key, updateRequest.id), ref)
    ref
  }

  val leader: Receive = {
    case Insert(key, value, id) ⇒ update(UpdateRequest(key, Some(value), id))
    case Remove(key, id) ⇒ update(UpdateRequest(key, None, id))
    case Get(key, id) ⇒ handleGet(key, id)
    case Replicas(replicas) ⇒
      val removedReplicators = replicaStore.add(replicas)
      self ! ReplicatorsTerminated(removedReplicators)
      replicateKV(replicaStore.replicators)
    case replicated@Replicated(key, id) ⇒ globalAcks.get((key, id)).foreach(_ forward replicated)
    case GlobalAck(key, id, replyTo, globalAck) ⇒
      globalAcks = globalAcks - ((key, id))
      if (globalAck) replyTo ! OperationAck(id)
      else replyTo ! OperationFailed(id)
    case replicatorsTerminated: ReplicatorsTerminated ⇒ globalAcks.values.foreach(_ ! replicatorsTerminated)
//    case Terminated(replica) ⇒ handleReplicatorsTerminated(replica)
    case _ ⇒
  }


  val replica: Receive = {
    case Get(key, id) ⇒ handleGet(key, id)
    case Snapshot(key, value, seq) ⇒ handleSnapshot(key, value, seq)
    case GlobalAck(key, id, replyTo, globalAck) ⇒
      globalAcks = globalAcks - ((key, id))
      if (globalAck) replyTo ! SnapshotAck(key, id)
    case _ ⇒
  }

  private def update(updateRequest: UpdateRequest): Unit = {
    import updateRequest._
    val request = UpdateRequest(key, value, id)
    val ackMgr = createGlobalAckManager(request)
    updateLocalStore(key, value)
    replicate(key, value, id)
    persistenceActor.tell(Persist(key, value, id), ackMgr)
  }

  private def handleSnapshot(key: String, value: Option[String], snapshotSeq: Long): Unit = {
    snapshotSeq match {
      case ValidSnapshot() ⇒
        val ref = createGlobalAckManager(UpdateRequest(key, value, snapshotSeq, onlyPersist = true))
        lastSnapshotSeq = snapshotSeq
        updateLocalStore(key, value)
        ref ! Persist(key, value, snapshotSeq)
      case OldOrDuplicateSnapshot() ⇒ sender ! SnapshotAck(key, snapshotSeq)
      case _ ⇒ // ignore invalid snapshots
    }
  }

  object ValidSnapshot {
    def unapply(seq: Long): Boolean = seq == (lastSnapshotSeq + 1)
  }

  object OldOrDuplicateSnapshot {
    def unapply(seq: Long): Boolean = seq <= lastSnapshotSeq
  }

  private def updateLocalStore(key: String, value: Option[String]): Unit =
    value match {
      case Some(value) ⇒ kv = kv.updated(key, value)
      case None ⇒ kv = kv - key
    }

  private def replicate(key: String, value: Option[String], id: Long): Unit =
    replicaStore.replicators.foreach(_ ! Replicate(key, value, id))

  // fixme
  private def replicateKV(replicators: Set[Replicator]): Unit =
    replicators.foreach { replicator ⇒
      kv.foreach {
        case (k, v) ⇒ replicator ! Replicate(k, Some(v), Random.nextLong())
      }
    }

  private def handleGet(key: String, id: Long): Unit = sender() ! GetResult(key, kv.get(key), id)

}

