package scala.actors.controlflow

import scala.actors._

/**
 * Provides useful methods for using asynchronous flow control.
 */
object ControlFlow {

  // Continuations

  /**
   * Create a continuation which, when applied, will run the given
   * function in a reaction of the then-current actor.
   */
  implicit def reactionCont(f: () => Unit)(implicit eh: ExceptionHandler): Cont[Unit] = new Cont[Unit] {
    def apply(value: Unit): Nothing = {
      val channel = new Channel[Unit](Actor.self)
      channel ! ()
      channel.react {
        case _: Any => handleCaught(f())(exceptionHandler)
      }
    }
    val exceptionHandler = eh
  }

  /**
   * Create a continuation which, when applied, will run the given
   * function in a reaction of the then-current actor.
   */
  implicit def reactionCont[R](f: R => Unit)(implicit eh: ExceptionHandler): Cont[R] = new Cont[R] {
    def apply(value: R): Nothing = {
      val channel = new Channel[Unit](Actor.self)
      channel ! ()
      channel.react {
        case _: Any => handleCaught(f(value))(exceptionHandler)
      }
    }
    val exceptionHandler = eh
  }

  /**
   * Create a continuation which, when applied, will exit the
   * then-current actor and run the given function in a new actor.
   */
  def actorCont[R](f: R => Unit)(implicit eh: ExceptionHandler) = new Cont[R] {
    def apply(value: R): Nothing = {
      Actor.actor { handleCaught(f(value))(exceptionHandler) }
      Actor.exit
    }
    val exceptionHandler = eh
  }

  /**
   * Create a continuation which, when applied, will run the given
   * function then exit the then-current actor. Calling the
   * continuation is relatively lightweight, but can cause the stack
   * to overflow.
   */
  def nestedCont[R](f: R => Unit)(implicit eh: ExceptionHandler) = new Cont[R] {
    def apply(value: R): Nothing = {
      handleCaught(f(value))(exceptionHandler)
      Actor.exit
    }
    val exceptionHandler = eh
  }

  // AsyncFunctions

  private[this] def handleCaught[R](body: => R)(implicit eh: ExceptionHandler): R = {
    try {
      body
    } catch {
      // XXX: Only allow scala.actors Throwables to pass through.
      case e: Exception => eh.handle(e)
      case e: Error => eh.handle(e)
    }
  }

  /**
   * Creates a Responder which evaluates the given AsyncFunction.
   */
/*  def respondOn[R](f: AsyncFunction0[R])(implicit eh: ExceptionHandler) = new Responder[R] {
    def respond(k: R => Unit): Nothing = {
      val cont = reactionCont(k)
      handleCaught(f(k))
    }
  }*/

  /**
   * Creates an AsyncFunction which evaluates the given Responder.
   */
/*  def respondOn[R](responder: Responder[R])(implicit eh: ExceptionHandler) = new AsyncFunction0[R] {
    def apply(k: Cont[R]) = {
      import k.exceptionHandler
      handleCaught(responder.respond { value: R => k(value) })
    }
  }*/

  implicit def directAsyncFunction[R](f: Function1[Cont[R], Nothing]):AsyncFunction0[R] = new AsyncFunction0[R] {
    def apply(k: Cont[R]) = {
      import k.exceptionHandler
      handleCaught(f(k))
    }
  }

  implicit def directAsyncFunction[T1, R](f: Function2[T1, Cont[R], Nothing]): AsyncFunction1[T1, R] = new AsyncFunction1[T1, R] {
    def apply(v1: T1)(k: Cont[R]) = {
      import k.exceptionHandler
      handleCaught(f(v1, k))
    }
  }

  /**
   * Converts the given Function0 into an AsyncFunction0 that, when
   * applied, passes the Function0's result to a
   * continuation. Exceptions thrown by the Function0 are passed to
   * the continuation's exception method.
   */
  def asAsync[R](f: Function0[R]) = new AsyncFunction0[R] {
    def apply(k: Cont[R]) = {
      import k.exceptionHandler
      k(handleCaught(f()))
    }
  }

  /**
   * Converts the given Function1 into an AsyncFunction1 that, when
   * applied, passes the Function1's result to a
   * continuation. Exceptions thrown by the Function1 are passed to
   * the continuation's exception method.
   */
  def asAsync[T1, R](f: Function1[T1, R]) = new AsyncFunction1[T1, R] {
    def apply(v1: T1)(k: Cont[R]) = {
      import k.exceptionHandler
      k(handleCaught(f(v1)))
    }
  }

  abstract sealed class AsyncResult[-R]
  case class NormalResult[R](value: R) extends AsyncResult[R]
  case class ExceptionResult(cause: Throwable) extends AsyncResult[Nothing]

  // a continuation which sends a Result to the given OutputChannel
  def sendingCont[R](oc: OutputChannel[E forSome { type E >: AsyncResult[R] } ]) = new Cont[R] {
    def apply(value: R): Nothing = {
      oc ! NormalResult(value)
      Actor.exit
    }
    val exceptionHandler = new ExceptionHandler {
      def handle(t: Throwable): Nothing = {
        oc ! ExceptionResult(t)
        Actor.exit // normal exit; error handled by continuation
      }
    }
  }

  class TimeoutException extends Exception

  def handleAsyncResult[R](msg: Any)(implicit eh: ExceptionHandler): R = msg match {
    case NormalResult(value) => value.asInstanceOf[R]
    case ExceptionResult(t) => eh.handle(t)
    case TIMEOUT => eh.handle(new TimeoutException)
    case unexpected => eh.handle(new MatchError("Expected NormalResult or ExceptionResult: " + unexpected))
  }

  def callWithCC[A](f: AsyncFunction0[A]): A = {
    implicit val exceptionHandler = ExceptionHandler.thrower
    val channel = new Channel[Any](Actor.self)
    val k = sendingCont(channel)
    Actor.actor { f(k) }
    val msg = channel.receive { case any => any }
    handleAsyncResult[A](msg)
  }

  def callWithCCWithin[A](msec: Long)(f: AsyncFunction0[A]): A = {
    implicit val exceptionHandler = ExceptionHandler.thrower
    val channel = new Channel[Any](Actor.self)
    val k = sendingCont(channel)
    Actor.actor { f(k) }
    val msg = channel.receiveWithin(msec) { case any => any }
    handleAsyncResult[A](msg)
  }

}
