package scala.actors.controlflow.concurrent

import scala.actors.controlflow.ControlFlow._

class AsyncLazyFuture[A](f: AsyncFunction0[A]) extends AsyncFuture[A] {

  private sealed trait State
  private case class Initial(f: AsyncFunction0[A]) extends State
  private case class Run(promise: AsyncPromise[A]) extends State
  
  private var state: State = Initial(f)

  def apply(fc: FC[A]): Nothing = synchronized {
    state match {
      case Initial(f) => {
        force
        apply(fc) // Reapply now that state has changed.
      }
      case Run(promise) => {
        promise(fc)
      }
    }
  }

  def force: Unit = synchronized {
    state match {
      case Initial(f) => {
        val promise = new AsyncPromise[A]
        state = Run(promise)
        Actor.actor { f((result: FunctionResult[A]) => promise.set(result)) }
      }
      case _ => ()
    }
  }
  
  def isSet: Boolean = synchronized {
    state match {
      case Initial(_) => false
      case Run(promise) => promise.isSet
    }
  }
  
  def result: Option[FunctionResult[A]] = synchronized {
    state match {
      case Initial(_) => None
      case Run(promise) => promise.result
    }
  }
}
