package scala.actors.controlflow

import scala.actors.controlflow.ControlFlow._

/**
 * "Function Continuations": contains the pair of continuations passed to
 * an AsyncFunction. The name is abbreviated to "FC" because it is used so
 * frequently.
 *
 * @param ret The continuation used to continue normally. An analogue of
 * Scala's <code>return</code> keyword.
 * @param thr The continuation used to continue with an error. An analogue of
 * Scala's <code>return</code> keyword.
 */
case class FC[-R] (ret: Cont[R], thr: Cont[Throwable]) {

  // Ensure well formed.
  assert(ret != null && thr != null)

  /**
   * Can be imported to introduce <code>thr</code> into the current scope.
   * Useful when combined with the methods of <code>ControlFlow</code>, many of
   * which accept an implicit <code>Cont[Throwable]</code>.
   *
   * <pre>
   * import fc.implicitThr
   * val fc2: FC[Int] = (i: Int) => ...
   * </pre>
   */
  implicit def implicitThr: Cont[Throwable] = thr

  /**
   * Creates a new FC which wraps this FC, guaranteeing that the given
   * AsyncFunction0 will execute before this FC is called. This is analogous
   * to Scala's finally clause.
   *
   * <pre>
   * val resource = ...
   * val finFC = fc.fin { resource.close }.toAsyncFunction
   * useResource(finFC)
   * </pre>
   */
  def fin(body: AsyncFunction0[Unit]): FC[R] = {
    // Run body before returning.
    val finRet: Cont[R] = { (value: R) =>
      body { () => ret(value) }
    }
    val finThr: Cont[Throwable] = { (t: Throwable) =>
      body { () => thr(t) }
    }
    FC(finRet, finThr)
  }
}
