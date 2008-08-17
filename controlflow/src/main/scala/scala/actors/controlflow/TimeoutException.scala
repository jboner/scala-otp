package scala.actors.controlflow

/**
 * Thrown when an operation fails to execute within the required time.
 *
 * @see AsyncFunction0#within(Long)
 * @see AsyncFunction1#within(Long)
 */
class TimeoutException extends Exception
