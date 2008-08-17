/*
 * FunctionResult.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scala.actors.controlflow

/**
 * Represents the result of a function call: either a value returned, or an
 * exception thrown.
 */
abstract sealed class FunctionResult[-R]
case class Return[R](value: R) extends FunctionResult[R]
case class Throw(throwable: Throwable) extends FunctionResult[Any]
