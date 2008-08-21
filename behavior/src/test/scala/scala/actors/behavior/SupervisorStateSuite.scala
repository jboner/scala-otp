/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.behavior

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest._

import scala.actors.Actor._

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class SupervisorStateSuite extends TestNGSuite {
  val dummyActor = new GenericServer { override def body: PartialFunction[Any, Unit] = { case _ => }}
  val newDummyActor = () => dummyActor
  var state: SupervisorState = _
  var proxy: GenericServerContainer = _
  var supervisor: Supervisor = _

  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod
  def setup = {
    proxy = new GenericServerContainer("server1", newDummyActor)
    object factory extends SupervisorFactory {
      override def getSupervisorConfig: SupervisorConfig = {
        SupervisorConfig(
          RestartStrategy(AllForOne, 3, 100),
          Worker(
            proxy,
            LifeCycle(Permanent, 100))
          :: Nil)
      }
    }
    supervisor = factory.newSupervisor
    state = new SupervisorState(supervisor, new AllForOneStrategy(3, 100))
  }

  @Test
  def testAddServer = {
    state.addServerContainer(proxy)
    state.getServerContainer("server1") match {
      case None => fail("should have returned server")
      case Some(server) =>
        assert(server != null)
        assert(server.isInstanceOf[GenericServerContainer])
        assert(proxy === server)
    }
  }

  @Test
  def testGetServer = {
    state.addServerContainer(proxy)
    state.getServerContainer("server1") match {
      case None => fail("should have returned server")
      case Some(server) =>
        assert(server != null)
        assert(server.isInstanceOf[GenericServerContainer])
        assert(proxy === server)
    }
  }

  @Test
  def testRemoveServer = {
    state.addServerContainer(proxy)

    state.removeServerContainer("server1")
    state.getServerContainer("server1") match {
      case Some(_) => fail("should have returned None")
      case None =>
    }
    state.getServerContainer("dummyActor") match {
      case Some(_) => fail("should have returned None")
      case None =>
    }
  }

  @Test
  def testGetNonExistingServerBySymbol = {
    state.getServerContainer("server2") match {
      case Some(_) => fail("should have returned None")
      case None =>
    }
  }

  @Test
  def testGetNonExistingServerByActor = {
    state.getServerContainer("dummyActor") match {
      case Some(_) => fail("should have returned None")
      case None =>
    }
  }
}
