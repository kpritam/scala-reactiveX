package kvstore.stores

import akka.actor.ActorRef
import kvstore.Replicator.Replicate

import scala.util.Random

class KVStore() {
  private var kv = Map.empty[String, String]

  def update(key: String, value: Option[String]): Unit =
    value match {
      case Some(value) ⇒ kv = kv.updated(key, value)
      case None ⇒ kv = kv - key
    }

  def get(key: String): Option[String] = kv.get(key)

  def replicate(replicators: Set[ActorRef]): Unit =
    replicators.foreach { replicator ⇒
      kv.foreach {
        case (k, v) ⇒ replicator ! Replicate(k, Some(v), Random.nextLong())
      }
    }
}
