package scala.actors.controlflow

/**
 * Encapsulates an action which should be performed when an exception
 * occurs. Each continuation has an ExceptionHandler, which can be
 * copied into other continuations that wish to inherit the same
 * behavior.
 */
trait ExceptionHandler {

  /**
   * Perform an action for the given Throwable. Never returns.
   */
  def handle(t: Throwable): Nothing
  
  /**
   * Create a new ExceptionHandler, based on this one, that first
   * attempts to 'catch' the Throwable with a given
   * PartialFunction. If the PartialFunction matches, then it is
   * evaluated, otherwise the original ExceptionHandler is called.
   */
  def catching(body: PartialFunction[Throwable, Nothing]) =
    new ExceptionHandler {
      def handle(t: Throwable) =
        if (body.isDefinedAt(t)) body(t)
        else ExceptionHandler.this.handle(t)
    }
}

object ExceptionHandler {

  /**
   * A simple ExceptionHandler that simple rethrows the given
   * exception when called.
   */
  def thrower = new ExceptionHandler {
    def handle(t: Throwable) = throw t
  }

}

