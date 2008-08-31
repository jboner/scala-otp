package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.concurrent._

/**
 * Utility class to help with building an AsyncStream.
 */
class AsyncStreamBuilder[A] {

  private var tail: AsyncPromise[AsyncStream[A]] = new AsyncPromise[AsyncStream[A]]

  /**
   * Retrieve the AsyncStream that is yet to be built.
   */
  def asyncTail: AsyncFuture[AsyncStream[A]] = synchronized { tail }

  def append(element: A): Unit = synchronized {
    val newTail = new AsyncPromise[AsyncStream[A]]
    val withElement = new AsyncStream[A] {
      def isEmpty = false
      def head = element
      def asyncTail = newTail
    }
    tail.set(Return(withElement))
    tail = newTail
  }
  
  def append(getStream: AsyncFunction0[AsyncStream[A]]): Unit = synchronized {
    val oldTail = tail
    val newTail = new AsyncPromise[AsyncStream[A]]
    Actor.actor {
      implicit val implicitThr: Cont[Throwable] = cont({ t: Throwable =>
        oldTail.set(Throw(t))
      })(thrower)
      getStream { stream: AsyncStream[A] =>
        stream.asyncAppend(newTail) { combined: AsyncStream[A] =>
          oldTail.set(Return(combined))
        }
      }
    }
    tail = newTail
  }

  def end: Unit = synchronized {
    tail.set(Return(AsyncStream.empty))
  }

}
