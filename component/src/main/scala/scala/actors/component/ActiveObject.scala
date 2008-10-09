/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.component

import scala.actors.behavior._

import java.lang.reflect.{Method, Field, InvocationHandler, Proxy, InvocationTargetException}
import java.lang.annotation.Annotation

import net.liftweb.util.{Can, Full, Empty, Failure}

sealed class ActiveObjectException(msg: String) extends RuntimeException(msg)
class ActiveObjectInvocationTimeoutException(msg: String) extends ActiveObjectException(msg)

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object ActiveObject {

  def newInstance[T](intf: Class[T] forSome {type T}, target: AnyRef, timeout: Int): T = {
    val proxy = new ActiveObjectProxy(target, timeout)
    start(proxy.server)
    newInstance(intf, proxy)
  }

  def newInstance[T](intf: Class[T] forSome {type T}, proxy: ActiveObjectProxy): T = {
    Proxy.newProxyInstance(
      proxy.target.getClass.getClassLoader,
      Array(intf),
      proxy).asInstanceOf[T]
  }

  def start(restartStrategy: RestartStrategy, components: List[Worker]): Supervisor = {
    object factory extends SupervisorFactory {
      override def getSupervisorConfig: SupervisorConfig = {
        SupervisorConfig(restartStrategy, components)
      }
    }
    val supervisor = factory.newSupervisor
    supervisor ! scala.actors.behavior.Start
    supervisor
  }

  private def start(server: GenericServerContainer): Supervisor = 
    start(
      RestartStrategy(OneForOne, 5, 1000),
      Worker(
        server,
        LifeCycle(Permanent, 100))
      :: Nil)

}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class ActiveObjectProxy(val target: AnyRef, val timeout: Int) extends InvocationHandler {
  private val oneway = classOf[scala.actors.annotation.oneway]

  private[ActiveObjectProxy] object dispatcher extends GenericServer {
    override def body: PartialFunction[Any, Unit] = {
      case invocation: Invocation =>
        try {
          reply(Full(invocation.invoke))
        } catch {
          case e: InvocationTargetException =>
            val cause = e.getTargetException
            Failure(cause.getMessage, Full(cause), Nil)
          case e =>
            Failure(e.getMessage, Full(e), Nil)
        }
      case 'exit =>  exit; reply(Empty)
      case unexpected => throw new ActiveObjectException("Unexpected message to actor proxy: " + unexpected)
    }
  }

  val server = new GenericServerContainer(target.getClass.getName, () => dispatcher)
  server.setTimeout(timeout)

  def invoke(proxy: AnyRef, m: Method, args: Array[AnyRef]): AnyRef = invoke(Invocation(m, args, target))

  def invoke(invocation: Invocation): AnyRef =  {
    if (invocation.method.isAnnotationPresent(oneway)) server ! invocation
    else {
      server.!!![Can[AnyRef]](invocation, { 
        val message = "proxy invocation timed out after " + timeout + " milliseconds"
        Failure(message, Full(new ActiveObjectInvocationTimeoutException(message)), Nil)
      }) match {
        case Full(result) => result.asInstanceOf[AnyRef]
        case Failure(message, Full(e), _) => throw e
        case Failure(message, Empty, _) => throw new ActiveObjectException(message)
        case Empty => {}
      }
    }
  }
}

/**
 * Represents a snapshot of the current invocation.
 * 
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class Invocation(val method: Method, val args: Array[Object], val target: AnyRef) {
  def invoke: AnyRef = method.invoke(target, args)

  override def toString: String = "Invocation [method: " + method.getName + ", args: " + args + ", target: " + target + "]"

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, method)
    result = HashCode.hash(result, args)
    result = HashCode.hash(result, target)
    result
  }

  override def equals(that: Any): Boolean = {
    that != null &&
    that.isInstanceOf[Invocation] &&
    that.asInstanceOf[Invocation].method == method &&
    that.asInstanceOf[Invocation].args == args
    that.asInstanceOf[Invocation].target == target
  }
}

