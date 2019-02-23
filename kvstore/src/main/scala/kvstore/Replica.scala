package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, Scheduler, Terminated, Timers}
import akka.util.Timeout
import kvstore.Arbiter._

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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers {

  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

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

  case class StopPersistRetries(key: String, id: Long)

  implicit val timeout: Timeout = Timeout(1.seconds)

  override def preStart(): Unit = arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary ⇒ context.become(leader)
    case JoinedSecondary ⇒ context.become(replica)
  }

  val leader: Receive = {
    case Insert(key, value, id) ⇒ (updateLocalStore andThen persistWithRetry) (UpdateRequest(key, Some(value), id))
    case Remove(key, id) ⇒ (updateLocalStore andThen persistWithRetry) (UpdateRequest(key, None, id))
    case Get(key, id) ⇒ handleGet(key, id)
    case Replicas(replicas) ⇒
      replicaToReplicatorMap = buildReplicasToReplicatorMap(replicas)
      replicas.foreach(context.watch)
    case Persisted(key, id) ⇒ handlePersisted(key, id, OperationAck(id))
    case StopPersistRetries(key, id) ⇒ handleStopPersistRetries(key, id, Some(OperationFailed(id)))
    case Terminated(ref) ⇒ replicaToReplicatorMap.get(ref).foreach(context.stop)
    case _ ⇒
  }

  val replica: Receive = {
    case Get(key, id) ⇒ handleGet(key, id)
    case Snapshot(key, value, seq) ⇒ handleSnapshot(key, value, seq)
    case Persisted(key, id) ⇒ handlePersisted(key, id, SnapshotAck(key, id))
    case StopPersistRetries(key, id) ⇒ handleStopPersistRetries(key, id, None)
    case _ ⇒
  }

  private def handlePersisted(key: String, id: Long, msg: Any): Unit = {
    val cancellableKey = key + id
    cancellables.get(cancellableKey).foreach {
      case (replyTo, cancellable) ⇒
        replyTo ! msg
        cancellable.cancel()
        cancellables = cancellables - cancellableKey
    }
  }

  private def handleStopPersistRetries(key: String, id: Long, msgOpt: Option[Any]): Unit = {
    val cancellableKey = key + id
    cancellables.get(cancellableKey).foreach {
      case (replyTo, cancellable) ⇒
        msgOpt.foreach(msg ⇒ replyTo ! msg)
        cancellable.cancel()
        cancellables = cancellables - cancellableKey
        timers.cancel(cancellableKey)
    }
  }

  private def handleSnapshot(key: String, value: Option[String], snapshotSeq: Long): Unit =
    snapshotSeq match {
      case ValidSnapshot() ⇒ lastSnapshotSeq = snapshotSeq; (updateLocalStore andThen persistWithRetry) (UpdateRequest(key, value, snapshotSeq))
      case OldOrDuplicateSnapshot() ⇒ sender ! SnapshotAck(key, snapshotSeq)
      case _ ⇒ // ignore invalid snapshots
    }

  object ValidSnapshot {
    def unapply(seq: Long): Boolean = seq == (lastSnapshotSeq + 1)
  }

  object OldOrDuplicateSnapshot {
    def unapply(seq: Long): Boolean = seq <= lastSnapshotSeq
  }

  case class UpdateRequest(key: String, value: Option[String], id: Long)

  private def updateLocalStore: UpdateRequest ⇒ UpdateRequest = updateRequest ⇒ {
    import updateRequest._
    value match {
      case Some(value) ⇒ kv = kv.updated(key, value)
      case None ⇒ kv = kv - key
    }
    updateRequest
  }

  private def buildReplicasToReplicatorMap(replicas: Set[SecondaryReplica]): Map[SecondaryReplica, Replicator] =
    replicas.map { replica ⇒
      replicaToReplicatorMap.get(replica) match {
        case Some(replicator) ⇒ replica → replicator
        case None ⇒ replica → (createReplicator andThen replicateKV) (replica)
      }
    }.toMap

  private def createReplicator: SecondaryReplica ⇒ Replicator = replica ⇒ {
    context.actorOf(Replicator.props(replica))
  }

  private def replicateKV: Replicator ⇒ Replicator = replicator ⇒ {
    kv.foreach {
      case (k, v) ⇒ replicator ! Replicate(k, Some(v), Random.nextLong())
    }
    replicator
  }

  private def handleGet(key: String, id: Long): Unit = sender() ! GetResult(key, kv.get(key), id)

  private def persistWithRetry: UpdateRequest ⇒ Unit = updateRequest ⇒ {
    import updateRequest._
    val cancellable = scheduler.schedule(0.millis, 100.millis, persistenceActor, Persist(key, value, id))
    cancellables = cancellables.updated(key + id, (sender(), cancellable))
    timers.startSingleTimer(key + id, StopPersistRetries(key, id), 1.seconds)
  }

}

