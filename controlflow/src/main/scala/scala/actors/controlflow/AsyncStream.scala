package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.concurrent._

/**
 * The AsyncStream object provides functions for defining and working
 * with asynchronous streams.
 */
object AsyncStream {

  /**
   * An empty AsyncStream.
   */
  val empty: AsyncStream[Nothing] = new AsyncStream[Nothing] {
    override def isEmpty: Boolean = true
    override def head: Nothing = throw new NoSuchElementException("head of empty stream")
    val asyncTail = new AsyncLazyFuture({ (fc: FC[AsyncStream[Nothing]]) =>
      fc.thr(new UnsupportedOperationException("asyncTail of empty stream"))
    })
  }

  /**
   * Create a stream from a given head and an AsyncFunction0 that
   * returns the tail. The tail is evaluated lazily.
   */
  def cons[A](hd: A, getTail: AsyncFunction0[AsyncStream[A]]) = new AsyncStream[A] {
    override def isEmpty = false
    def head = hd
    val asyncTail = new AsyncLazyFuture(getTail)
  }

  /**
   * Convert the given list into an AsyncStream.
   */
  def fromList[A](list: List[A]): AsyncStream[A] =
    if (list.isEmpty) AsyncStream.empty
    else cons(list.head, (() => AsyncStream.fromList(list.tail)).toAsyncFunction)

  /**
   * Convert the given iterator into an AsyncStream.
   */
  def fromIterator[A](iterator: Iterator[A]): AsyncStream[A] =
    if (iterator.hasNext) cons(iterator.next, (() => AsyncStream.fromIterator(iterator)).toAsyncFunction)
    else AsyncStream.empty

  /**
   * Convert the given iterable into an AsyncStream.
   */
  def fromIterable[A](iterable: Iterable[A]): AsyncStream[A] =
    fromIterator(iterable.elements)

  def fromAsyncIterator[A](itr: AsyncIterator[A])(fc: FC[AsyncStream[A]]): Nothing = {
    import fc.implicitThr
    itr.hasNext { (hasNext: Boolean) =>
      if (hasNext) {
        itr.next { (next: A) =>
          fc.ret(AsyncStream.cons(next, fromAsyncIterator(itr) _))
        }
      } else fc.ret(AsyncStream.empty)
    }
  }
}

/**
 * A stream where the tail is evaluated asynchronously and returned
 * via a continuation. Synchronous access to the tail is still
 * available, but this is relatively heavyweight, since it uses
 * <code>callWithCC</code> internally.
 */
trait AsyncStream[+A] {

  def isEmpty: Boolean

  def head: A
  
  protected def addDefinedElems(buf: StringBuilder, prefix: String): StringBuilder = {
    val buf1 = buf.append(prefix).append(head)
    asyncTail.result match {
      case None => buf1 append ", ?"
      case Some(result) => result.toFunction.apply.addDefinedElems(buf1, ", ")
    }
  }

  def asyncElements: AsyncIterator[A] = new AsyncIterator[A] {
    var current = AsyncStream.this
    def hasNext(fc: FC[Boolean]) = {
      val result = !current.isEmpty
      fc.ret(result)
    }
    def next(fc: FC[A]) = {
      import fc.implicitThr
      current.asyncTail { (tl: AsyncStream[A]) =>
        val result = current.head
        current = tl
        fc.ret(result)
      }
    }
  }
  
  /**
   * Convert this AsyncStream into a synchronous Stream. Calling
   * <code>tail</code> will block the current thread, while evaluation occurs
   * in a different actor.
   */
  def toStream: Stream[A] = new Stream[A] {
    
    override def isEmpty = AsyncStream.this.isEmpty
    
    def head = AsyncStream.this.head

    def tail = {
      fc: FC[AsyncStream[A]] => AsyncStream.this.asyncTail(fc)
    }.toFunction.apply.toStream
    
    protected def addDefinedElems(buf: StringBuilder, prefix: String): StringBuilder =
      AsyncStream.this.addDefinedElems(buf, prefix)
  }

  /**
   * Get the tail of the list and return it asynchronously, via the
   * given continuation.
   */
  def asyncTail: AsyncFuture[AsyncStream[A]]

  /**
   * Convert the stream to a list. The result is returned
   * asynchronously, via the given continuation.
   */
  def asyncToList(fc: FC[List[A]]): Nothing = {
    // Construct a reversed list by concatenating each element to the
    // head as it is revealed. Then reverse this list to obtain the
    // correct ordering and return the result.
    import fc.implicitThr
    def toReversedList(s: AsyncStream[A], accum: List[A])(fc2: FC[List[A]]): Nothing = {
      if (s.isEmpty) fc2.ret(accum)
      else s.asyncTail { tl: AsyncStream[A] => toReversedList(tl, s.head :: accum)(fc2) }
    }
    toReversedList(this, Nil) { reversed: List[A] => fc.ret(reversed.reverse) }
  }

  /**
   * Perform a 'map' operation on the stream, using the given
   * AsyncFunction1. The result is returned asynchronously, via a
   * continuation.
   */
  def asyncMap[B](f: AsyncFunction1[A, B])(fc: FC[AsyncStream[B]]): Nothing = {
    AsyncStream.fromAsyncIterator(asyncElements.asyncMap(f))(fc)
  }

  def asyncFlatMap[B](f: AsyncFunction1[A, AsyncStream[B]])(fc: FC[AsyncStream[B]]): Nothing = {
    import fc.implicitThr
    val itrF: AsyncFunction1[A, AsyncIterator[B]] = { (a: A, fc: FC[AsyncIterator[B]]) =>
      f(a) { (bStream: AsyncStream[B]) =>
        fc.ret(bStream.asyncElements)
      }
    }
    AsyncStream.fromAsyncIterator(asyncElements.asyncFlatMap(itrF))(fc)
  }
  
  def asyncFilter(p: AsyncFunction1[A, Boolean])(fc: FC[AsyncStream[A]]): Nothing = {
    AsyncStream.fromAsyncIterator(asyncElements.asyncFilter(p))(fc)
  }
  
  def asyncForeach(f: AsyncFunction1[A, Unit])(fc: FC[Unit]): Nothing = {
    asyncElements.asyncForeach(f)(fc)
  }

  def asyncFoldLeft[B](z: B)(op: AsyncFunction1[(B, A), B])(fc: FC[B]): Nothing = {
    asyncElements.asyncFoldLeft(z)(op)(fc)
  }
  /**
   * Concatenate two streams. The second stream is made available as
   * the result of an AsyncFunction0. The result is returned
   * asynchronously, via a continuation.
   */
  def asyncAppend[B >: A](restGetter: AsyncFunction0[AsyncStream[B]])(fc: FC[AsyncStream[B]]): Nothing = {
    import fc.implicitThr
    if (isEmpty) restGetter(fc)
    else fc.ret(AsyncStream.cons(head, { fc2: FC[AsyncStream[B]] =>
      asyncTail { tl: AsyncStream[A] => tl.asyncAppend(restGetter)(fc2) }
    }))
  }
}
