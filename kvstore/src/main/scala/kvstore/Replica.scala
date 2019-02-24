package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, Scheduler, Terminated, Timers}
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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers {

  import Replica._
  import Replicator._

  type SecondaryReplica = ActorRef
  type Replicator = ActorRef

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var replicaToReplicatorMap = Map.empty[SecondaryReplica, Replicator]
  // the current set of replicators
  var replicators = Set.empty[Replicator]
  var lastSnapshotSeq: Long = -1L

  val persistenceActor: ActorRef = context.actorOf(persistenceProps, "persistence-actor")
  val scheduler: Scheduler = context.system.scheduler
  var cancellables = Map.empty[String, (ActorRef, Cancellable)]
  var prManagers = Map.empty[(String, Long), ActorRef]

  case class StopPersistRetries(key: String, id: Long)

  implicit val timeout: Timeout = Timeout(1.seconds)

  override def preStart(): Unit = arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary ⇒ context.become(leader)
    case JoinedSecondary ⇒ context.become(replica)
  }

  def createPRManager(updateRequest: UpdateRequest): ActorRef = {
    val ref = context.actorOf(GlobalAckManager.props(updateRequest, sender(), persistenceActor, replicators.size))
    prManagers = prManagers.updated((updateRequest.key, updateRequest.id), ref)
    ref
  }

  val leader: Receive = {
    case Insert(key, value, id) ⇒ update(UpdateRequest(key, Some(value), id))
    case Remove(key, id) ⇒ update(UpdateRequest(key, None, id))
    case Get(key, id) ⇒ handleGet(key, id)
    case Replicas(replicas) ⇒
      replicaToReplicatorMap = buildReplicasToReplicatorMap(replicas)
      replicas.foreach(context.watch)
    case replicated@Replicated(key, id) ⇒ prManagers.get((key, id)).foreach(_ ! replicated)
    case GlobalAck(key, id, replyTo, globalAck) ⇒
      prManagers = prManagers - ((key, id))
      if (globalAck) replyTo ! OperationAck(id)
      else replyTo ! OperationFailed(id)
    case Terminated(ref) ⇒ replicaToReplicatorMap.get(ref).foreach(context.stop)
    case _ ⇒
  }

  val replica: Receive = {
    case Get(key, id) ⇒ handleGet(key, id)
    case Snapshot(key, value, seq) ⇒ handleSnapshot(key, value, seq)
    case GlobalAck(key, id, replyTo, globalAck) ⇒
      prManagers = prManagers - ((key, id))
      if (globalAck) replyTo ! SnapshotAck(key, id)
    case _ ⇒
  }

  private def update(updateRequest: UpdateRequest): Unit = {
    import updateRequest._
    val request = UpdateRequest(key, value, id)
    val prMgr = createPRManager(request)
    updateLocalStore(key, value)
    replicate(key, value, id)
    persistenceActor.tell(Persist(key, value, id), prMgr)
  }

  private def handleSnapshot(key: String, value: Option[String], snapshotSeq: Long): Unit = {
    snapshotSeq match {
      case ValidSnapshot() ⇒
        val ref = createPRManager(UpdateRequest(key, value, snapshotSeq, onlyPersist = true))
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
    replicators.foreach(_ ! Replicate(key, value, id))

  private def buildReplicasToReplicatorMap(replicas: Set[SecondaryReplica]): Map[SecondaryReplica, Replicator] = {
    replicaToReplicatorMap = replicaToReplicatorMap.filter {
      case (replica, replicator) ⇒
        if (replicas.contains(replica)) true
        else {
          context.stop(replicator)
          false
        }
    }

    replicas.filterNot(_.compareTo(self) == 0).map { replica ⇒
      replicaToReplicatorMap.get(replica) match {
        case Some(replicator) ⇒ replica → replicator
        case None ⇒
          val replicator = (createReplicator andThen replicateKV) (replica)
          replicators = replicators + replicator
          replica → replicator
      }
    }.toMap
  }

  private def createReplicator: SecondaryReplica ⇒ Replicator = replica ⇒ {
    context.actorOf(Replicator.props(replica))
  }

  // fixme
  private def replicateKV: Replicator ⇒ Replicator = replicator ⇒ {
    kv.foreach {
      case (k, v) ⇒ replicator ! Replicate(k, Some(v), Random.nextLong())
    }
    replicator
  }

  private def handleGet(key: String, id: Long): Unit = sender() ! GetResult(key, kv.get(key), id)

}

