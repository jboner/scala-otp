package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

/**
 * The AsyncStream object provides functions for defining and working
 * with asynchronous streams.
 */
object AsyncStream {

  /**
   * An empty AsyncStream.
   */
  val empty = new AsyncStream[Nothing] {
    def isEmpty: Boolean = true
    def head: Nothing = throw new NoSuchElementException("head of empty stream")
    def tail(k: Cont[AsyncStream[Nothing]]): Nothing = k.exception(new UnsupportedOperationException("tail of empty stream"))
  }

  /**
   * Create a stream from a given head and an AsyncFunction0 that
   * returns the tail. The tail is evaluated lazily.
   */
  def cons[A](hd: A, getTail: AsyncFunction0[AsyncStream[A]]) = new AsyncStream[A] {
    override def isEmpty = false
    def head = hd
    private var tlCache: AsyncStream[A] = _
    def tail(k: Cont[AsyncStream[A]]): Nothing = {
      import k.exceptionHandler
      if (tlCache eq null) {
        getTail { tl: AsyncStream[A] => tlCache = tl; k(tl) }
      } else {
        k(tlCache)
      }
    }
  }

  /**
   * Convert the given list into an AsyncStream.
   */
  def fromList[A](list: List[A]): AsyncStream[A] =
    if (list.isEmpty) AsyncStream.empty
    else cons(list.head, { k: Cont[AsyncStream[A]] => k(AsyncStream.fromList(list.tail)) })
}

/**
 * A stream where the tail is evaluated asynchronously and returned
 * via a continuation.
 */
trait AsyncStream[+A] {

  /**
   * Determine if the list is empty.
   */
  def isEmpty: Boolean

  /**
   * Get the head of the list.
   */
  def head: A

  /**
   * Get the tail of the list and return it asynchronously, via the
   * given continuation.
   */
  def tail(k: Cont[AsyncStream[A]]): Nothing

  /**
   * A blocking operation which returns the current stream. The tail
   * of the stream is evaluated asynchronously in a different actor.
   */
  def toStream: Stream[A] =
    if (isEmpty) Stream.empty
    else Stream.cons(head, callWithCC(tail _).toStream)

  /**
   * Convert the stream to a list. The result is returned
   * asynchronously, via the given continuation.
   */
  def toList(k: Cont[List[A]]): Nothing = {
    // Construct a reversed list by concatenating each element to the
    // head as it is revealed. Then reverse this list to obtain the
    // correct ordering and return the result.
    import k.exceptionHandler
    def toReversedList(s: AsyncStream[A], accum: List[A])(k2: Cont[List[A]]): Nothing = {
      if (s.isEmpty) k2(accum)
      else s.tail { tl: AsyncStream[A] => toReversedList(tl, s.head :: accum)(k2) }
    }
    toReversedList(this, Nil) { reversed: List[A] => k(reversed.reverse) }
  }

  /**
   * Perform a 'map' operation on the stream, using the given
   * AsyncFunction1. The result is returned asynchronously, via a
   * continuation.
   */
  def asyncMap[B](f: AsyncFunction1[A, B])(k: Cont[AsyncStream[B]]): Nothing = {
    import k.exceptionHandler
    if (isEmpty) k(AsyncStream.empty)
    else f(head) { mappedHead: B =>
      k(AsyncStream.cons(mappedHead, { k2: Cont[AsyncStream[B]] =>
        tail {tl: AsyncStream[A] => tl.asyncMap(f)(k2) }
      }))
    }
  }

  /**
   * Concatenate two streams. The second stream is made available as
   * the result of an AsyncFunction0. The result is returned
   * asynchronously, via a continuation.
   */
  def append[B >: A](restGetter: AsyncFunction0[AsyncStream[B]])(k: Cont[AsyncStream[B]]): Nothing = {
    import k.exceptionHandler
    if (isEmpty) restGetter(k)
    else k(AsyncStream.cons(head, { k2: Cont[AsyncStream[B]] =>
      tail { tl: AsyncStream[A] => tl.append(restGetter)(k2) }
    }))
  }
}
