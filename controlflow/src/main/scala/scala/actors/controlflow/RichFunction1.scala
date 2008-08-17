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
 * An extension of a <code>Function1</code> that provides support for
 * asynchronous operations.
 */
trait RichFunction1[-T1, +R] extends Function1[T1, R] {

  /**
   * Creates an asynchronous version of this function.
   */
  def toAsyncFunction: AsyncFunction1[T1, R]

}
