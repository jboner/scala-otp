package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

object AsyncIterator {
  
  val empty = new AsyncIterator[Nothing] {
    def hasNext(fc: FC[Boolean]) = fc.ret(false)
    def next(fc: FC[Nothing]) = fc.thr(new NoSuchElementException("next on empty iterator"))
  }
  
}

trait AsyncIterator[+A] {

  def hasNext(fc: FC[Boolean]): Nothing

  def next(fc: FC[A]): Nothing
  
  def asyncMap[B](f: AsyncFunction1[A, B]) = new AsyncIterator[B] {
    def hasNext(fc: FC[Boolean]) = AsyncIterator.this.hasNext(fc)
    def next(fc: FC[B]) = {
      val origNext: AsyncFunction0[A] = AsyncIterator.this.next _
      (origNext andThen f)(fc)
    }
  }

  def asyncFlatMap[B](f: AsyncFunction1[A, AsyncIterator[B]]): AsyncIterator[B] = new AsyncIterator[B] {
    private var cur: AsyncIterator[B] = AsyncIterator.empty
    private def handle[C](
      curHasNextHandler: => AsyncFunction0[C],
      origHasNextHandler: => AsyncFunction0[C],
      noNextHandler: => AsyncFunction0[C])(fc: FC[C]): Nothing = {
      import fc.implicitThr
      cur.hasNext { (curHasNext: Boolean) =>
        if (curHasNext) curHasNextHandler(fc)
        else {
          AsyncIterator.this.hasNext { (origHasNext: Boolean) =>
            if (origHasNext) {
              val origNext: AsyncFunction0[A] = AsyncIterator.this.next _
              (origNext andThen f) { (fResult: AsyncIterator[B]) =>
                cur = fResult
                origHasNextHandler(fc)
              }
            } else {
              noNextHandler(fc)
            }
          }
        }
      }
    }
    def hasNext(fc: FC[Boolean]) = {
      handle[Boolean](
        Return(true).toAsyncFunction,
        hasNext _,
        Return(false).toAsyncFunction
      )(fc)
    }
    def next(fc: FC[B]) = {
      handle[B](
        cur.next _,
        next _,
        Throw(new NoSuchElementException("next on empty iterator")).toAsyncFunction
      )(fc)
    }
  }
  
  
  def asyncFilter(p: AsyncFunction1[A, Boolean]) = new AsyncIterator[A] {

    private var loaded: Option[A] = None

    private def loadNext(fc: FC[Unit]): Nothing = {
      loaded match {
        case s: Some[A] => fc.ret(())
        case None => {
          import fc.implicitThr
          AsyncIterator.this.hasNext { (origHasNext: Boolean) =>
            if (origHasNext) {
              AsyncIterator.this.next { (origNext: A) =>
                p(origNext) { (matches: Boolean) =>
                  if (matches) {
                    loaded = Some(origNext)
                    fc.ret(())
                  } else {
                    loadNext(fc)
                  }
                }
              }
            } else {
              // Reached end.
              fc.ret(())
            }
          }
        }
      }
    }

    def hasNext(fc: FC[Boolean]) = {
      import fc.implicitThr
      loadNext { () =>
        loaded match {
          case Some(_) => fc.ret(true)
          case None => fc.ret(false)
        }
      }
    }
    def next(fc: FC[A]) = {
      import fc.implicitThr
      loadNext { () =>
        loaded match {
          case Some(element) => {
            loaded = None
            fc.ret(element)
          }
          case None => fc.thr(new NoSuchElementException)
        }
      }
    }
  }
 
  def asyncForeach(f: AsyncFunction1[A, Unit])(fc: FC[Unit]): Nothing = {
    import fc.implicitThr
    AsyncIterator.this.hasNext { (origHasNext: Boolean) =>
      if (origHasNext) {
        AsyncIterator.this.next { (a: A) =>
          f(a) { () =>
            asyncForeach(f)(fc)
          }
        }
      } else fc.ret(())
    }
  }

}
