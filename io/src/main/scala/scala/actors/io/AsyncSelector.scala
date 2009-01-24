package scala.actors.io

import java.nio.channels._
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

object AsyncSelector {
  abstract sealed class Operation { val mask: Int }
  case object Accept extends Operation { val mask = SelectionKey.OP_ACCEPT }
  case object Connect extends Operation { val mask = SelectionKey.OP_CONNECT }
  case object Read extends Operation { val mask = SelectionKey.OP_READ }
  case object Write extends Operation { val mask = SelectionKey.OP_WRITE }
  private[AsyncSelector] val OPS = List(Accept, Connect, Read, Write)
}

/**
 * Wraps a <code>Selector</code>, adding support for callbacks when operations
 * are ready.
 *
 * @param selector The <code>Selector</code> to wrap.
 */
class AsyncSelector(val selector: Selector) {
// XXX: Implement stop/cancel?
  import AsyncSelector._
  //println("AsyncSelector: Creating.")

  def this() = this(Selector.open)

  private var registrationCount: Int = 0

  /**
   * While running, the registrations
   */
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
        var opMap = key.attachment.asInstanceOf[Map[Operation,Queue[FC[Unit]]]]
        //println("AsyncSelector: "+ key + " readyOps: " + actualReadyOps)
        // Process each ready op
        for (op <- OPS if ((actualReadyOps & op.mask) != 0)) {
          //println("AsyncSelector: " + op + " ready.")
          val opEntry = opMap(op)
          for (fc <- opEntry) {
            registrationsFinished += 1
            //println("AsyncSelector: calling fc.")
            Actor.actor { fc.ret(()) }
          }
          opMap = opMap - op
        }
        // Update key
        val newInterestOps = currentInterestOps & ~actualReadyOps
        //println("AsyncSelector: "+ key + " newInterestOps: " + newInterestOps)
        key.interestOps(newInterestOps)
        key.attach(opMap)
      }      
    }

    // Terminate actor if no current registrations.
    synchronized {
      registrationCount -= registrationsFinished
      //println("AsyncSelector: registrationCount: " + registrationCount)
      if (registrationCount == 0) {
        //println("AsyncSelector: exiting.")
        selectActor = None
        Actor.exit
      }
    }

    selectionLoop // Hopefully tail-recursive. :-)
  }

  // note: method blocks until operation registered - could run in new actor?
  def register(ch: SelectableChannel, op: Operation)(fc: FC[Unit]): Nothing = {
    try {
      val key = ch.register(selector, 0, Map())
      key.synchronized { // XXX: Synchronize on ch.blockingLock instead?
        val currentInterestOps = key.interestOps
        val newInterestOps = currentInterestOps | op.mask
        //println("AsyncSelector: currentInterestOps: " + currentInterestOps)
        //println("AsyncSelector: newInterestOps: " + newInterestOps)
        if (newInterestOps != currentInterestOps) {
          //println("AsyncSelector: setting interestOps: " + newInterestOps)
          key.interestOps(newInterestOps)
        }
        val oldOpMap = key.attachment.asInstanceOf[Map[Operation,Queue[FC[Unit]]]]
        val oldOpEntry = oldOpMap.getOrElse(op, Queue.Empty)
        val newOpEntry = oldOpEntry + fc
        val newOpMap = oldOpMap + ((op, newOpEntry))
        key.attach(newOpMap)
      }
    } catch {
      // FC cannot have been called previously, since attach has failed.
      case e: Exception => fc.thr(e)
    }
    // Start actor if first registration.
    synchronized {
      if (registrationCount == 0) {
        //println("AsyncSelector: starting.")
        selectActor = Some(Actor.actor { selectionLoop })
      }
      registrationCount += 1
      //println("AsyncSelector: registrationCount: " + registrationCount)
    }
    Actor.exit
  }

}
