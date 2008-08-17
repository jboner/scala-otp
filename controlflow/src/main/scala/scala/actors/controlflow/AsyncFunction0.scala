package scala.actors.controlflow

import scala.actors.Actor
import scala.actors.controlflow.ControlFlow._

/**
 * A function taking no arguments where the result is provided
 * asynchronously via a continuation.
 */
trait AsyncFunction0[+R] extends AnyRef {

  /**
   * Apply this function. The result will be provided via one of the given
   * <code>FC</code>'s continuations: either <code>ret</code> or
   * <code>thr</code>.
   */
  def apply(fc: FC[R]): Nothing

  /**
   * Create a function which executes this function then passes its result to
   * the given function.
   */
  def andThen[A](g: AsyncFunction1[R, A]) = new AsyncFunction0[A] {
    def apply(fc: FC[A]) = {
      assert(fc != null)
      import fc.implicitThr
      AsyncFunction0.this.apply { result: R => g(result)(fc) }
    }
  }

  /**
   * Apply this function in a separate actor. sending the function's result
   * as a <code>FunctionResult</code> down the returned <code>Channel</code>.
   */
  private def applyInActor: Channel[Any] = {
    val channel = new Channel[Any](Actor.self)
    Actor.actor {
      AsyncFunction0.this.apply { result: FunctionResult[R] =>
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
  def within(msec: Long): AsyncFunction0[R] = new AsyncFunction0[R] {
    def apply(fc: FC[R]): Nothing = {
      assert(fc != null)
      val channel = AsyncFunction0.this.applyInActor
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
  def toFunction: RichFunction0[R] = new RichFunction0[R] {
    def apply: R = {
      val channel = AsyncFunction0.this.applyInActor
      channel.receive {
        case msg: Any => handleResultMessage(msg)
      }
    }

    def toAsyncFunction = AsyncFunction0.this
  }
}
