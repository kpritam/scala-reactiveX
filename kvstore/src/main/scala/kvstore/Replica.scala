package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, Scheduler, Terminated, Timers}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.GlobalAckManager.GlobalAck
import kvstore.Persistence.Persist
import kvstore.stores.{KVStore, ReplicaStore}

import scala.concurrent.duration._

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

  val kvStore: KVStore = new KVStore()
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
      kvStore.replicate(replicaStore.replicators)
    case replicated@Replicated(key, id) ⇒ globalAcks.get((key, id)).foreach(_ forward replicated)
    case GlobalAck(key, id, replyTo, globalAck) ⇒
      globalAcks = globalAcks - ((key, id))
      if (globalAck) replyTo ! OperationAck(id)
      else replyTo ! OperationFailed(id)
    case replicatorsTerminated: ReplicatorsTerminated ⇒ globalAcks.values.foreach(_ ! replicatorsTerminated)
    case Terminated(replica) ⇒
      val replicators = replicaStore.remove(Set(replica))
      self ! ReplicatorsTerminated(replicators)
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
    kvStore.update(key, value)
    replicate(key, value, id)
    persistenceActor.tell(Persist(key, value, id), ackMgr)
  }

  private def handleSnapshot(key: String, value: Option[String], snapshotSeq: Long): Unit = {
    def isOldOrDuplicateSnapshot = snapshotSeq <= lastSnapshotSeq

    def isValidSnapshot = snapshotSeq == lastSnapshotSeq + 1

    if (isValidSnapshot) {
      val ref = createGlobalAckManager(UpdateRequest(key, value, snapshotSeq, onlyPersist = true))
      lastSnapshotSeq = snapshotSeq
      kvStore.update(key, value)
      ref ! Persist(key, value, snapshotSeq)
    } else if (isOldOrDuplicateSnapshot) sender ! SnapshotAck(key, snapshotSeq)
  }

  private def replicate(key: String, value: Option[String], id: Long): Unit =
    replicaStore.replicators.foreach(_ ! Replicate(key, value, id))

  private def handleGet(key: String, id: Long): Unit = sender() ! GetResult(key, kvStore.get(key), id)
}

