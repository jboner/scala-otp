package scala.actors.controlflow

import scala.actors._

/**
 * Provides useful methods for using asynchronous flow control.
 */
object ControlFlow {

  // Continuations

  /**
   * Creates a continuation which, when applied, will run the given
   * function in a reaction of the then-current actor. If the function
   * throws an exception, then this will be passed to <code>thr</code>.
   *
   * <pre>
   * implicit val implicitThr = thrower
   * val c: Cont[Unit] = () => println("Called")
   * </pre>
   */
  implicit def cont(f: () => Unit)(implicit thr: Cont[Throwable]): Cont[Unit] = {
    assert(thr != null)
    new Cont[Unit] {
      def apply(value: Unit) = {
        inReaction {
          try {
            f()
            Actor.exit
          } catch {
            case t if !isControlFlowThrowable(t) => thr(t)
          }
        }
      }
    }
  }

  /**
   * Creates a continuation which, when applied, will run the given
   * function in a reaction of the then-current actor. If the function
   * throws an exception, then this will be passed to <code>thr</code>.
   *
   * <pre>
   * implicit val implicitThr = thrower
   * val c: Cont[R] = (value: R) => println("Called: " + value)
   * </pre>
   */
  implicit def cont[R](f: R => Unit)(implicit thr: Cont[Throwable]): Cont[R] = {
    new Cont[R] {
      def apply(value: R): Nothing = {
        inReaction {
          try {
            f(value)
            Actor.exit
          } catch {
            case t if !isControlFlowThrowable(t) => thr(t)
          }
        }
      }
    }
  }

  /**
   * Avoid overflowing the stack by running the given code in a reaction.
   * There may be a more efficient way to do this.
   */
  private def inReaction(body: => Unit): Nothing = {
    val channel = new Channel[Unit](Actor.self)
    channel ! ()
    channel.react {
      case _: Any => body
    }
  }
  
  // Throwable continuations

  /**
   * Creates a simple Cont[Throwable] that rethrows the given exception when
   * applied.
   *
   * <pre>
   * implicit val implicitThr = thrower
   * val c: Cont[R] = (value: R) => println("Called: " + value)
   * </pre>
   */
  def thrower = new Cont[Throwable] {
    def apply(t: Throwable) = throw t
  }

  // FCs

  /**
   * Creates an FC with a return continuation, which, when applied, will run the
   * given function in a reaction of the then-current actor. If the function
   * throws an exception, then this will be passed to <code>thr</code>.
   *
   * <pre>
   * implicit val implicitThr = thrower
   * val fc: FC[Unit] = () => println("Called")
   * </pre>
   */
  implicit def reactionFC(f: () => Unit)(implicit thr: Cont[Throwable]): FC[Unit] = {
    val ret: Cont[Unit] = cont(f)(thr)
    FC(ret, thr)
  }

  /**
   * Creates an FC with a return continuation, which, when applied, will run the
   * given function in a reaction of the then-current actor. If the function
   * throws an exception, then this will be passed to <code>thr</code>.
   *
   * <pre>
   * implicit val implicitThr = thrower
   * val fc: FC[R] = (value: R) => println("Called: " + value)
   * </pre>
   */
  implicit def reactionFC[R](f: R => Unit)(implicit thr: Cont[Throwable]): FC[R] = {
    val ret: Cont[R] = cont(f)(thr)
    FC(ret, thr)
  }
  
  /**
   * Creates an FC with continuations that supply a <code>FunctionResult</code>
   * to the given function.
   *
   * <pre>
   * val fc: FC[R] = (result: FunctionResult[R]) => channel ! result
   * </pre>
   */
  implicit def resultFC[R](f: FunctionResult[R] => Unit): FC[R] = {
    // Introduce 'thrower' to avoid recursive use of 'thr'.
    val thr: Cont[Throwable] = cont((t: Throwable) => f(Throw(t)))(thrower)
    implicit val implicitThr = thr
    val retF = (r: R) => f(Return(r))
    val ret: Cont[R] = cont(retF)(thr)
    FC(ret, thr)
  }

  // AsyncFunctions

  /**
   * Creates an <code>AsyncFunction0</code> directly from a function with an
   * equivalent signature.
   *
   * <pre>
   * val af: AsyncFunction0[R] => (fc: FC[R]) => fc.ret(...)
   * </pre>
   */
  implicit def asyncFunction0[R](f: Function1[FC[R], Nothing]): AsyncFunction0[R] = new AsyncFunction0[R] {
    def apply(fc: FC[R]) = {
      assert(fc != null)
      inReaction {
        try {
          f(fc)
        } catch {
          case t if !isControlFlowThrowable(t) => fc.thr(t)
        }
      }
    }
  }

  /**
   * Creates an <code>AsyncFunction1</code> directly from a function with an
   * equivalent signature.
   *
   * <pre>
   * val af: AsyncFunction1[T1, R] => (v1: T1, fc: FC[R]) => fc.ret(...)
   * </pre>
   */
  implicit def asyncFunction1[T1, R](f: Function2[T1, FC[R], Nothing]): AsyncFunction1[T1, R] = new AsyncFunction1[T1, R] {
    def apply(v1: T1)(fc: FC[R]) = {
      assert(fc != null)
      inReaction {
        try {
          f(v1, fc)
        } catch {
          case t if !isControlFlowThrowable(t) => fc.thr(t)
        }
      }
    }
  }

  // Functions

  /**
   * Converts a <code>Function0</code> into a <code>RichFunction0</code>.
   */
  implicit def richFunction0[R](f: Function0[R]) = new RichFunction0[R] {
    self =>

    def apply = f()

    def resultApply: FunctionResult[R] = {
      try {
        val returnValue = f()
        Return(returnValue)
      } catch {
        case t if !isControlFlowThrowable(t) => Throw(t)
      }
    }

    def toAsyncFunction = new AsyncFunction0[R] {
      def apply(fc: FC[R]) = {
        assert(fc != null)
        resultApply.toAsyncFunction(fc)
      }

      override def toFunction: RichFunction0[R] = self

    }
    
  }
  
  /**
   * Converts a <code>Function1</code> into a <code>RichFunction1</code>.
   */
  implicit def richFunction1[T1, R](f: Function1[T1, R]) = new RichFunction1[T1, R] {
    self =>

    def apply(v1: T1) = f(v1)

    def resultApply(v1: T1): FunctionResult[R] = {
      try {
        val returnValue = f(v1)
        Return(returnValue)
      } catch {
        case t if !isControlFlowThrowable(t) => Throw(t)
      }
    }

    def toAsyncFunction = new AsyncFunction1[T1, R] {
      def apply(v1: T1)(fc: FC[R]) = {
        assert(fc != null)
        resultApply(v1).toAsyncFunction(fc)
      }

      override def toFunction: RichFunction1[T1, R] = self
    }

  }
  
  // Misc

  /**
   * Determine whether or not a given throwable is used by
   * <code>scala.actors</code> to manage for control flow.
   *
   * <p>Currently this is done by treating any non-Exception, non-Error
   * throwable as a control flow throwable. Ideally we need a common superclass
   * for control flow throwables, to make matching correct.
   */
  def isControlFlowThrowable(t: Throwable): Boolean = {
    t match {
      case e: Exception => false
      case e: Error => false
      case _ => true
    }
  }

}
