package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

/**
 * A function where the result is provided asynchronously, via a
 * continuation.
 */
trait AsyncFunction0[+R] extends AnyRef {
  def apply(k: Cont[R]): Nothing

  def andThen[A](g: AsyncFunction1[R, A]) = new AsyncFunction0[A] {
    def apply(k: Cont[A]) = {
      import k.exceptionHandler
      AsyncFunction0.this.apply { result: R => g(result)(k) }
    }
  }
}
