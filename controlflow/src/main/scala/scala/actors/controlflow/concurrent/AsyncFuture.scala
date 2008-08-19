package scala.actors.controlflow.concurrent

import scala.actors.controlflow.ControlFlow._
import scala.collection.immutable.Queue

trait AsyncFuture[A] extends AsyncFunction0[A] {
  
  def isSet: Boolean
  
  def result: Option[FunctionResult[A]]

}
