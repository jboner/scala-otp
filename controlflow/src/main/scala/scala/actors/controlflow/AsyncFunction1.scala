package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

/**
 * A function taking one argument where the result is provided
 * asynchronously via a continuation.
 */
trait AsyncFunction1[-T1, +R] extends AnyRef {

  /**
   * Apply this function. The result will be provided via one of the given
   * <code>FC</code>'s continuations: either <code>ret</code> or
   * <code>thr</code>.
   */
  def apply(v1: T1)(fc: FC[R]): Nothing

  /**
   * Create a function which executes the given function then passes its result
   * to this function.
   */
  def compose[A](g: AsyncFunction1[A, T1]) = new AsyncFunction1[A, R] {
    def apply(x: A)(fc: FC[R]) = {
      assert(fc != null)
      import fc.implicitThr
      g(x) { result: T1 => AsyncFunction1.this.apply(result)(fc) }
    }
  }

  /**
   * Create a function which executes this function then passes its result to
   * the given function.
   */
  def andThen[A](g: AsyncFunction1[R, A]) = new AsyncFunction1[T1, A] {
    def apply(x: T1)(fc: FC[A]) = {
      assert(fc != null)
      import fc.implicitThr
      AsyncFunction1.this.apply(x) { result: R => g(result)(fc) }
    }
  }
  
  /**
   * Apply this function in a separate actor. sending the function's result
   * as a <code>FunctionResult</code> down the returned <code>Channel</code>.
   */
  private def applyInActor(v1: T1): Channel[Any] = {
    val channel = new Channel[Any](Actor.self)
    Actor.actor {
      AsyncFunction1.this.apply(v1) { result: FunctionResult[R] =>
        channel ! result
      }
    }
    channel
  }
  
  /**
   * Handle the message returned by <code>applyInActor</code>.
   */
  private def handleResultMessage(msg: Any): R = msg match {
    case Return(value) => value.asInstanceOf[R]
    case Throw(t) => throw t
    case TIMEOUT => throw new TimeoutException()
    case unknown => throw new MatchError(unknown)
  }
  
  /**
   * Creates a function which wraps this function, adding with a timeout
   * feature. The new function has the same behaviour as this function except
   * that it will continue with a <code>TimeoutException</code> if it takes
   * longer than <code>msec</code> milliseconds to execute.
   */
  def within(msec: Long): AsyncFunction1[T1, R] = new AsyncFunction1[T1, R] {
    def apply(v1: T1)(fc: FC[R]): Nothing = {
      assert(fc != null)
      val channel = applyInActor(v1)
      channel.reactWithin(msec) {
        case msg: Any => {
          try {
            val returnValue = handleResultMessage(msg)
            fc.ret(returnValue)
          } catch {
            case t if !isControlFlowThrowable(t) => fc.thr(t)
          }
        }
      }
    }
  }
  
  /**
   * Creates a synchronous version of this function. When executed, the function
   * will suspend the current thread and run the underlying asynchronous in a
   * new actor.
   */
  def toFunction: RichFunction1[T1, R] = new RichFunction1[T1, R] {
    def apply(v1: T1): R = {
      val channel = applyInActor(v1)
      channel.receive {
        case msg: Any => handleResultMessage(msg)
      }
    }

    def toAsyncFunction = AsyncFunction1.this
  }
}
