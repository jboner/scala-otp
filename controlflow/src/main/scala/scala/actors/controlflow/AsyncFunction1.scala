package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

/**
 * A function that takes an argument and where the result is provided
 * asynchronously, via a continuation.
 */
trait AsyncFunction1[-T1, +R] extends AnyRef {
  def apply(v1: T1)(k: Cont[R]): Nothing

  def compose[A](g: AsyncFunction1[A, T1]) = new AsyncFunction1[A, R] {
    def apply(x: A)(k: Cont[R]) = {
      import k.exceptionHandler
      g(x) { result: T1 => AsyncFunction1.this.apply(result)(k) }
    }
  }

  def andThen[A](g: AsyncFunction1[R, A]) = new AsyncFunction1[T1, A] {
    def apply(x: T1)(k: Cont[A]) = {
      import k.exceptionHandler
      AsyncFunction1.this.apply(x) { result: R => g(result)(k) }
    }
  }
}
