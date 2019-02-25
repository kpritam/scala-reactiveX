package kvstore

import akka.actor.{ActorContext, ActorRef}

class ReplicaStore(context: ActorContext) {
  type Replica = ActorRef
  type Replicator = ActorRef

  private var _replicaToReplicatorMap = Map.empty[Replica, Replicator]
  private var _replicators = Set.empty[Replicator]

  def replicaToReplicatorsMap: Map[Replica, Replicator] = _replicaToReplicatorMap

  def replicators: Set[Replicator] = _replicators

  def add(replicas: Set[Replica]): Set[Replicator] = {
    //1. start watching new replicas
    val newReplicas = (replicas diff _replicaToReplicatorMap.keySet).filterNot(_.compareTo(context.self) == 0)
    newReplicas.foreach(context.watch)

    val removedReplicators = remove(_replicaToReplicatorMap.keySet diff replicas)

    //2. create replicators for new replicas
    _replicaToReplicatorMap = _replicaToReplicatorMap ++ newReplicas.map { replica ⇒
      val replicator = context.actorOf(Replicator.props(replica), s"replicator-${replica.path.name}")
      _replicators = _replicators + replicator
      replica → replicator
    }.toMap

    //3. return removed replicators
    removedReplicators
  }

  // remove replicas and their corresponding replicators
  def remove(replicas: Set[Replica]): Set[Replicator] = {
    val removedReplicators: Set[ActorRef] = _replicaToReplicatorMap.filter {
      case (replica, _) ⇒ if (replicas.contains(replica)) true else false
    }.values.toSet

    removedReplicators.foreach(context.stop)
    _replicaToReplicatorMap = _replicaToReplicatorMap filterKeys replicas
    _replicators = _replicators -- removedReplicators
    removedReplicators
  }
}



