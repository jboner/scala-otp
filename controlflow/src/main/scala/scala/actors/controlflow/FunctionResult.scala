/*
 * FunctionResult.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scala.actors.controlflow

/**
 * Represents the result of a function call: either a value returned, or an
 * exception thrown. Extends <code>Function0</code> and
 * <code>AsyncFunction0</code>, allowing it to be introduced into computations.
 */
sealed trait FunctionResult[+R] {
  def toFunction: RichFunction0[R]
  def toAsyncFunction: AsyncFunction0[R]
}

case class Return[+R](value: R) extends FunctionResult[R] {
  def toFunction = new RichFunction0[R] {
    def apply: R = value
    def resultApply: FunctionResult[R] = Return.this
    override def toAsyncFunction = Return.this.toAsyncFunction
  }
  def toAsyncFunction = new AsyncFunction0[R] {
    def apply(fc: FC[R]): Nothing = fc.ret(value)
    override def toFunction = Return.this.toFunction
  }
}

case class Throw(throwable: Throwable) extends FunctionResult[Nothing] {
  def toFunction = new RichFunction0[Nothing] {
    def apply: Nothing = throw throwable
    def resultApply: FunctionResult[Nothing] = Throw.this
    override def toAsyncFunction = Throw.this.toAsyncFunction
  }
  def toAsyncFunction = new AsyncFunction0[Nothing] {
    def apply(fc: FC[Nothing]): Nothing = fc.thr(throwable)
    override def toFunction = Throw.this.toFunction
  }
}
