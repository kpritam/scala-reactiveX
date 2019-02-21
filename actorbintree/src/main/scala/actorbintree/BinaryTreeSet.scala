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
    compare(
      op = insertQuery,
      gt = insert(Right, insertQuery),
      lt = insert(Left, insertQuery),
      matched = {
        if (removed) removed = false
        sendOperationFinishedMsg(insertQuery)
      })


  private def insert(position: Position, insertQuery: Insert): Unit =
    forwardOrAct(
      pos = position,
      nonLeaf = _ ! insertQuery,
      leaf = {
        createChild(position, insertQuery.elem)
        sendOperationFinishedMsg(insertQuery)
      }
    )

  private def handleRemove(removeQuery: Remove): Unit =
    compare(
      op = removeQuery,
      gt = remove(Right, removeQuery),
      lt = remove(Left, removeQuery),
      matched = {
        removed = true
        sendOperationFinishedMsg(removeQuery)
      })

  def remove(position: Position, removeQuery: Remove): Unit =
    forwardOrAct(
      pos = position,
      nonLeaf = _ ! removeQuery,
      leaf = sendOperationFinishedMsg(removeQuery)
    )

  private def handleContains(containsQuery: Contains): Unit =
    compare(
      op = containsQuery,
      gt = contains(Right, containsQuery),
      lt = contains(Left, containsQuery),
      matched = containsQuery.requester ! ContainsResult(containsQuery.id, result = !removed)
    )

  def contains(position: Position, containsQuery: Contains): Unit =
    forwardOrAct(
      pos = position,
      nonLeaf = _ ! containsQuery,
      leaf = containsQuery.requester ! ContainsResult(containsQuery.id, result = false)
    )

  private def compare(op: Operation, gt: ⇒ Unit, lt: ⇒ Unit, matched: ⇒ Unit): Unit =
    if (op.elem > elem) gt
    else if (op.elem < elem) lt
    else matched

  private def forwardOrAct(pos: Position, nonLeaf: ActorRef ⇒ Unit, leaf: ⇒ Unit): Unit =
    subtrees.get(pos) match {
      case Some(ref) ⇒ nonLeaf(ref)
      case None ⇒ leaf
    }

  private def createChild(position: Position, elm: Int): Unit = {
    val ref = context.actorOf(BinaryTreeNode.props(elm, initiallyRemoved = false))
    subtrees += (position → ref)
  }

  private def sendOperationFinishedMsg(op: Operation): Unit = op.requester ! OperationFinished(op.id)
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
