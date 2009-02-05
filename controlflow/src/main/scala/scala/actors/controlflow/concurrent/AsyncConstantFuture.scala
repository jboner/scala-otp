package scala.actors.controlflow.concurrent

import scala.actors.controlflow.ControlFlow._
import scala.collection.immutable.Queue

/**
 * A simple <code>AsyncFuture</code> with an immediate, constant
 * value. Arguably not really a future. :-)
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class AsyncConstantFuture[A](value: FunctionResult[A]) extends AsyncFuture[A] {

  def apply(fc: FC[A]): Nothing = value.toAsyncFunction.apply(fc)

  def isSet: Boolean = true
  
  def result: Option[FunctionResult[A]] = Some(value)
}
