package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

object ControlFlowTestHelper {

  /**
   * Run a body of test code asynchronously. If the test takes longer than
   * <code>msec</code> milliseconds to run, then throw a
   * <code>TimeoutException</code>.
   */
  def asyncTest(msec: Long)(body: => Unit) = {
    (() => body).toAsyncFunction.within(msec).toFunction.apply
  }

}
