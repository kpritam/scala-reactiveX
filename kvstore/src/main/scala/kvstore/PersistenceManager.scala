package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, Scheduler}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.PersistenceManager.PersistSuccess
import kvstore.Replica.UpdateRequest

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationLong

object PersistenceManager {

  case class PersistSuccess(key: String, id: Long)

  def props(updateRequest: UpdateRequest, persistenceActor: ActorRef) =
    Props(new PersistenceManager(updateRequest, persistenceActor))
}

class PersistenceManager(updateRequest: UpdateRequest, persistenceActor: ActorRef)
  extends Actor {

  import updateRequest._

  val scheduler: Scheduler = context.system.scheduler
  implicit val ec: ExecutionContextExecutor = context.dispatcher

  val cancellable: Cancellable = scheduler.schedule(0.millis, 100.millis, persistenceActor, Persist(key, value, id))

  override def receive: Receive = {
    case persist@Persist ⇒ persistenceActor ! persist
    case Persisted(`key`, `id`) ⇒
      context.parent ! PersistSuccess(key, id)
      cancellable.cancel()
      context.stop(self)
  }

  override def postStop(): Unit = {
    cancellable.cancel()
    ()
  }

}
