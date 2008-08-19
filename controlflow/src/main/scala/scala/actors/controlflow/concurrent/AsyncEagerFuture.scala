package scala.actors.controlflow.concurrent

import scala.actors.controlflow.ControlFlow._

class AsyncEagerFuture[A](f: AsyncFunction0[A]) extends AsyncFuture[A] {

  private val promise = new AsyncPromise[A]

  Actor.actor { f((result: FunctionResult[A]) => promise.set(result)) }

  def apply(fc: FC[A]): Nothing = promise(fc)
  
  def isSet: Boolean = promise.isSet
  
  def result: Option[FunctionResult[A]] = promise.result

}