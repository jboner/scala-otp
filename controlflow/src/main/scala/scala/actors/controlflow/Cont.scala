package scala.actors.controlflow

import scala.actors._

/**
 * A continuation, representing a future computation.
 */
trait Cont[-R] {

  def apply(value: R): Nothing

  final def exception(t: Throwable): Nothing = exceptionHandler.handle(t)

  implicit val exceptionHandler: ExceptionHandler

}
