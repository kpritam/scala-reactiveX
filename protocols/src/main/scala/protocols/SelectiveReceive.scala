package protocols

import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorContext, _}

object SelectiveReceive {
  /**
    * @return A behavior that stashes incoming messages unless they are handled
    *         by the underlying `initialBehavior`
    * @param bufferSize      Maximum number of messages to stash before throwing a `StashOverflowException`
    *                        Note that 0 is a valid size and means no buffering at all (ie all messages should
    *                        always be handled by the underlying behavior)
    * @param initialBehavior Behavior to decorate
    * @tparam T Type of messages
    *
    *           Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
    *           `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
    */
  def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = Behaviors.setup { ctx ⇒
    val buffer = StashBuffer[T](bufferSize)

    def interpret(behavior: Behavior[T], msg: T): (Behavior[T], Boolean) = {
      val started = Behavior.validateAsInitial(Behavior.start(behavior, ctx))
      val interpreted = Behavior.interpretMessage(started, ctx, msg)
      (Behavior.canonicalize(interpreted, started, ctx), Behavior.isUnhandled(interpreted))
    }

    def beh(current: Behavior[T]): ExtensibleBehavior[T] = new ExtensibleBehavior[T] {
      override def receive(_ctx: ActorContext[T], msg: T): Behavior[T] = {
        val (canonical, unHandled) = interpret(current, msg)
        if (unHandled) {
          println(s"Stashing $msg")
          buffer.stash(msg)
          beh(canonical)
        }
        else buffer.unstashAll(ctx, beh(canonical))
      }

      override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
        println(s"Processing signal $msg")
        beh(Behavior.interpretSignal(current, ctx, msg))
      }
    }

    beh(initialBehavior)
  }

}
