package scala.actors.io

import java.nio.channels._
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

object RichSelector {
  abstract sealed class Operation { val mask: Int }
  case object Accept extends Operation { val mask = SelectionKey.OP_ACCEPT }
  case object Connect extends Operation { val mask = SelectionKey.OP_CONNECT }
  case object Read extends Operation { val mask = SelectionKey.OP_READ }
  case object Write extends Operation { val mask = SelectionKey.OP_WRITE }
  private[RichSelector] val OPS = List(Accept, Connect, Read, Write)
}

// XXX: Implement stop/cancel?
class RichSelector(val selector: Selector) {
  import RichSelector._
  //println("RichSelector: Creating.")

  def this() = this(Selector.open)

  private var registrationCount: Int = 0
  private var selectActor: Option[Actor] = None

  private[this] def selectionLoop: Unit = {

    selector.select(1500)

    var registrationsFinished = 0

    for (key <- selector.selectedKeys) {
      key.synchronized { // XXX: Synchronize on ch.blockingLock instead?
        // Key state
        val currentInterestOps = key.interestOps
        val readyOps = key.readyOps
        val actualReadyOps = key.interestOps & key.readyOps // readyOps is not always updated by the Selector
        var opMap = key.attachment.asInstanceOf[Map[Operation,Queue[Cont[Unit]]]]
        //println("RichSelector: "+ key + " readyOps: " + actualReadyOps)
        // Process each ready op
        for (op <- OPS if ((actualReadyOps & op.mask) != 0)) {
          //println("RichSelector: " + op + " ready.")
          val opEntry = opMap(op)
          for (k <- opEntry) {
            registrationsFinished += 1
            //println("RichSelector: calling k.")
            Actor.actor { k(()) }
          }
          opMap = opMap - op
        }
        // Update key
        val newInterestOps = currentInterestOps & ~actualReadyOps
        //println("RichSelector: "+ key + " newInterestOps: " + newInterestOps)
        key.interestOps(newInterestOps)
        key.attach(opMap)
      }      
    }

    // Terminate actor if no current registrations.
    synchronized {
      registrationCount -= registrationsFinished
      //println("RichSelector: registrationCount: " + registrationCount)
      if (registrationCount == 0) {
        //println("RichSelector: exiting.")
        selectActor = None
        Actor.exit
      }
    }

    selectionLoop // Hopefully tail-recursive. :-)
  }

  // note: method blocks until operation registered - could callback instead?
  def register(ch: SelectableChannel, op: Operation)(k: Cont[Unit]): Unit = {
    println("RichSelector: registering for " + op + ".")
    val key = ch.register(selector, 0, Map())
    key.synchronized { // XXX: Synchronize on ch.blockingLock instead?
      val currentInterestOps = key.interestOps
      val newInterestOps = currentInterestOps | op.mask
      //println("RichSelector: currentInterestOps: " + currentInterestOps)
      //println("RichSelector: newInterestOps: " + newInterestOps)
      if (newInterestOps != currentInterestOps) {
        //println("RichSelector: setting interestOps: " + newInterestOps)
        key.interestOps(newInterestOps)
      }
      val oldOpMap = key.attachment.asInstanceOf[Map[Operation,Queue[Cont[Unit]]]]
      val oldOpEntry = oldOpMap.getOrElse(op, Queue.Empty)
      val newOpEntry = oldOpEntry + k
      val newOpMap = oldOpMap + ((op, newOpEntry))
      key.attach(newOpMap)
    }
    // Start actor if first registration.
    synchronized {
      if (registrationCount == 0) {
        //println("RichSelector: starting.")
        selectActor = Some(Actor.actor { selectionLoop })
      }
      registrationCount += 1
      //println("RichSelector: registrationCount: " + registrationCount)
    }
  }

}
