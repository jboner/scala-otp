package scala.actors.controlflow

import scala.actors._

/**
 * A continuation, representing a future computation.
 */
trait Cont[-R] {

  /**
   * Continue with this continuation, supplying the given value to the future
   * computation. This method never exits normally; it will always throw an
   * exception. These exceptions are used to manage the control flow, so should
   * never be caught by the calling code. Calling code can use
   * <code>ControlFlow.isControlFlowThrowable()</code> to determine whether or
   * not an exception can be caught.
   *
   * @param value The value to supply to the following computation.
   */
  def apply(value: R): Nothing

}
