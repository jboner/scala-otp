/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.component

import scala.actors.behavior._

import java.lang.reflect.{Method, Field, InvocationHandler, Proxy, InvocationTargetException}
import java.lang.annotation.Annotation

sealed class ActiveObjectException(msg: String) extends RuntimeException(msg)
class ActiveObjectInvocationTimeoutException(msg: String) extends ActiveObjectException(msg)

case class Component(component: ActiveObjectProxy, lifeCycle: LifeCycle) extends Server

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object ActiveObject {

  def newInstance[T](intf: Class[T] forSome {type T}, target: AnyRef, timeout: Int): T = {
    val proxy = new ActiveObjectProxy(target, timeout)
    supervise(proxy)
    newInstance(intf, proxy)
  }

  def newInstance[T](intf: Class[T] forSome {type T}, proxy: ActiveObjectProxy): T = {
    Proxy.newProxyInstance(
      proxy.target.getClass.getClassLoader,
      Array(intf),
      proxy).asInstanceOf[T]
  }

  def supervise(restartStrategy: RestartStrategy, components: List[Component]): Supervisor = {
  	object factory extends SupervisorFactory {
      override def getSupervisorConfig: SupervisorConfig = {
        SupervisorConfig(restartStrategy, components.map(c => Worker(c.component.server, c.lifeCycle)))
      }
    }
    val supervisor = factory.newSupervisor
    supervisor ! scala.actors.behavior.Start
    supervisor
  }

  private def supervise(proxy: ActiveObjectProxy): Supervisor = 
    supervise(
      RestartStrategy(OneForOne, 5, 1000),
      Component(
        proxy,
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
          reply(ErrRef(invocation.invoke))
        } catch {
          case e: InvocationTargetException => ErrRef(e.getTargetException)
          case e => ErrRef(e)
        }
      case 'exit =>  exit; reply()
      case unexpected => throw new ActiveObjectException("Unexpected message to actor proxy: " + unexpected)
    }
  }

  private[component] val server = new GenericServerContainer(target.getClass.getName, () => dispatcher)
  server.setTimeout(timeout)
 
  //private[ActiveObjectProxy] val timeOutException = { throw new ActiveObjectInvocationTimeoutException("proxy invocation timed out after " + timeout + " milliseconds") }
  
  def invoke(proxy: AnyRef, m: Method, args: Array[AnyRef]): AnyRef = invoke(Invocation(m, args, target))

  def invoke(invocation: Invocation): AnyRef =  {
    if (invocation.method.isAnnotationPresent(oneway)) server ! invocation
    else {
      val result: ErrRef[AnyRef] = server !!! (invocation, ErrRef({ throw new ActiveObjectInvocationTimeoutException("proxy invocation timed out after " + timeout + " milliseconds") })) 
      try { result() } catch { case e => e.printStackTrace; throw e }
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

/**
 * Reference that can hold either a typed value or an exception.
 *
 * Usage:
 * <pre>
 * scala> ErrRef(1)
 * res0: ErrRef[Int] = ErrRef@a96606
 *  
 * scala> res0()
 * res1: Int = 1
 *
 * scala> res0() = 3
 *
 * scala> res0()
 * res3: Int = 3
 * 
 * scala> res0() = { println("Hello world"); 3}
 * Hello world
 *
 * scala> res0()
 * res5: Int = 3
 *  
 * scala> res0() = error("Lets see what happens here...")
 *
 * scala> res0()
 * java.lang.RuntimeException: Lets see what happens here...
 * 	at ErrRef.apply(RefExcept.scala:11)
 * 	at .<init>(<console>:6)
 * 	at .<clinit>(<console>)
 * 	at Re...
 * </pre>
 */
class ErrRef[S](s: S){
  private[this] var contents: Either[Throwable, S] = Right(s)
  def update(value: => S) = contents = try { Right(value) } catch { case (e : Throwable) => Left(e) }
  def apply() = contents match {
    case Right(s) => s
    case Left(e) => throw e.fillInStackTrace
  }
  override def toString(): String = "ErrRef[" + contents + "]"
}
object ErrRef {
  def apply[S](s: S) = new ErrRef(s)
}

