/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import akka.actor._

import scala.collection.immutable.Queue
import scala.util.Random

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insertQuery an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to forwardOrActAndReply garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insertQuery or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {

  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root: ActorRef = createRoot

  // optional
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC ⇒ startGC()
    case msg: Operation ⇒ root ! msg
    case msg ⇒ println(s"Unknown message : [$msg] received.")
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case msg: Operation ⇒ pendingQueue = pendingQueue :+ msg
    case CopyFinished ⇒ finishedGC(newRoot)
    case GC ⇒ // ignore
  }

  private def startGC(): Unit = {
    val newRoot = createRoot
    root ! CopyTo(newRoot)
    context.become(garbageCollecting(newRoot))
  }

  private def finishedGC(newRoot: ActorRef): Unit = {
    root = newRoot
    pendingQueue.foreach(root ! _)
    pendingQueue = Queue.empty[Operation]
    context.become(normal)
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(new BinaryTreeNode(elem, initiallyRemoved))
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees: Map[Position, ActorRef] = Map[Position, ActorRef]()
  var removed: Boolean = initiallyRemoved
  var copyFinishedCount = 0

  // optional
  def receive: Receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case insertQuery: Insert ⇒ handleInsert(insertQuery)
    case containsQuery: Contains ⇒ handleContains(containsQuery)
    case removeQuery: Remove ⇒ handleRemove(removeQuery)
    case copy@CopyTo(ref) ⇒
      if (!removed) ref ! Insert(self, Random.nextInt(), elem) // if not removed, re-insert to new root
      if (subtrees.isEmpty) sendCopyFinishedToParentAndStop() // if leaf node
      else subtrees.values.foreach(_ ! copy)
    case CopyFinished ⇒
      copyFinishedCount += 1
      if (copyFinishedCount == subtrees.size) sendCopyFinishedToParentAndStop()
    case _ ⇒ // ignore OperationReply messages
  }

  private def handleInsert(insertQuery: Insert): Unit =
    conditionalAct(insertQuery, insert, {
      if (removed) removed = false
      OperationFinished(insertQuery.id)
    })

  private def insert(position: Position, insertQuery: Insert): Unit =
    forwardOrActAndReply(
      position,
      insertQuery, {
        createChild(position, insertQuery.elem)
        OperationFinished(insertQuery.id)
      }
    )

  private def handleRemove(removeQuery: Remove): Unit =
    conditionalAct(removeQuery, remove, {
      removed = true
      OperationFinished(removeQuery.id)
    })

  def remove(position: Position, removeQuery: Remove): Unit =
    forwardOrActAndReply(position, removeQuery, OperationFinished(removeQuery.id))

  private def handleContains(containsQuery: Contains): Unit =
    conditionalAct(containsQuery, contains, ContainsResult(containsQuery.id, result = !removed))

  def contains(position: Position, containsQuery: Contains): Unit =
    forwardOrActAndReply(position, containsQuery, ContainsResult(containsQuery.id, result = false))


  private def conditionalAct[T <: Operation](operation: T, action: (Position, T) ⇒ Unit, reply: ⇒ OperationReply): Unit =
    if (operation.elem > elem) action(Right, operation)
    else if (operation.elem < elem) action(Left, operation)
    else operation.requester ! reply

  private def forwardOrActAndReply(position: Position, operation: Operation, actAndReply: ⇒ OperationReply): Unit =
    subtrees.get(position) match {
      case Some(ref) ⇒ ref ! operation
      case None ⇒ operation.requester ! actAndReply
    }

  private def createChild(position: Position, elm: Int): Unit = {
    val ref = context.actorOf(BinaryTreeNode.props(elm, initiallyRemoved = false))
    subtrees += (position → ref)
  }

  private def sendCopyFinishedToParentAndStop(): Unit = {
    context.parent ! CopyFinished
    context.stop(self)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???

}
