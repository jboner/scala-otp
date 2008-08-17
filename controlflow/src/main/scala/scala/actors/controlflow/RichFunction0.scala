/*
 * RichFunction0.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scala.actors.controlflow

import scala.actors._
import scala.actors.controlflow.ControlFlow._

/**
 * An extension of a <code>Function0</code> that provides support for
 * asynchronous operations.
 */
trait RichFunction0[+R] extends Function0[R] {

  /**
   * Creates an asynchronous version of this function.
   */
  def toAsyncFunction: AsyncFunction0[R]

}
