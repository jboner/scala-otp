package scala.actors.controlflow.concurrent

import scala.actors.controlflow.ControlFlow._
import scala.collection.immutable.Queue

class AsyncLock {
  
  sealed trait State
  case object Unlocked extends State
  case class Locked(q: Queue[FC[Unit]]) extends State

  private var state: State = Unlocked

  def lock(fc: FC[Unit]): Nothing = synchronized {
    state match {
      case Unlocked => {
        state = Locked(Queue.Empty)
        fc.ret(())
      }
      case Locked(q) => {
        state = Locked(q + fc)
        Actor.exit
      }
    }
  }

  def tryLock(fc: FC[Boolean]): Nothing = synchronized {
    state match {
      case Unlocked => {
        state = Locked(Queue.Empty)
        fc.ret(true)
      }
      case Locked(q) => {
        fc.ret(false)
      }
    }
  }

  def unlock: Unit = synchronized {
    state match {
      case Locked(q) => {
        if (q.isEmpty) {
          state = Unlocked
        } else {
          val (head, newQ) = q.dequeue
          state = Locked(newQ)
        }
      }
      case s => throw new IllegalStateException(s.toString)
    }
  }

  def withLock[R](f: AsyncFunction0[R]): AsyncFunction0[R] = {
    { (fc: FC[R]) =>
      import fc.implicitThr
      lock { () =>
        f(fc.withFinally((() => unlock).toAsyncFunction))
      }
    }
  }
}
