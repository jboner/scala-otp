package scala.actors.controlflow.concurrent

import scala.actors.controlflow.ControlFlow._
import scala.collection.immutable.Queue

class AsyncPromise[A] extends AsyncFuture[A] {

  private sealed trait State
  private case class Unset(pending: Queue[FC[A]]) extends State
  private case class Set(result: FunctionResult[A]) extends State
  
  private var state: State = Unset(Queue.Empty)

  def apply(fc: FC[A]): Nothing = synchronized {
    state match {
      case Unset(pending) => {
        state = Unset(pending + fc)
        Actor.exit
      }
      case Set(result) => {
        result.toAsyncFunction.apply(fc)
      }
    }
  }
  
  def set(result: FunctionResult[A]): Unit = synchronized {
    state match {
      case Unset(pending) => {
        state = Set(result)
        val resultFunction = result.toAsyncFunction
        for (fc <- pending) {
          Actor.actor {
            resultFunction(fc)
          }
        }
      }
      case s => throw new IllegalStateException(s.toString)
    }
  }

  def isSet: Boolean = synchronized {
    state match {
      case Unset(pending) => false
      case Set(value) => true
    }
  }
  
  def result: Option[FunctionResult[A]] = synchronized {
    state match {
      case Unset(_) => None
      case Set(result) => Some(result)
    }
  }
}

