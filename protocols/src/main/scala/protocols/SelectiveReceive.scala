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
    var currentUncanonical = initialBehavior
    var current = Behavior.validateAsInitial(Behavior.start(initialBehavior, ctx))

    new ExtensibleBehavior[T] {
      override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
        println(s"Processing $msg")
        currentUncanonical = Behavior.interpretMessage(current, ctx, msg)
        current = Behavior.canonicalize(currentUncanonical, current, ctx)
        if (Behavior.isUnhandled(currentUncanonical)) {
          println(s"Stashing $msg")
          buffer.stash(msg)
        }
        else if(buffer.nonEmpty) {
          println("Unstashing ..")
          buffer.foreach(x ⇒ print(s"$x, "))
          println()
          buffer.unstashAll(ctx.asInstanceOf[scaladsl.ActorContext[T]], this)
        }
        this
      }

      override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
        println(s"Processing signal $msg")
        Behavior.interpretSignal(current, ctx, msg)
      }
    }
    //    new SelectiveRec[T](buffer, Behavior.validateAsInitial(Behavior.start(initialBehavior, ctx)))
  }

  /*
  class SelectiveRec[T](stashBuffer: StashBuffer[T], initialBeh: Behavior[T]) extends ExtensibleBehavior[T] {
    override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
      val currentUncanonical = Behavior.interpretMessage(initialBeh, ctx, msg)
      val current = Behavior.canonicalize(currentUncanonical, initialBeh, ctx)
      if (Behavior.isUnhandled(currentUncanonical)) stashBuffer.stash(msg)
      else stashBuffer.unstashAll(ctx.asInstanceOf[scaladsl.ActorContext[T]], this)
      new SelectiveRec[T](stashBuffer, current)
    }

    override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] =
      Behavior.interpretSignal(this, ctx, msg)
  }
*/

}
