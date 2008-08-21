/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.behavior

sealed abstract class TestMessage
case object Ping extends TestMessage
case object Pong extends TestMessage
case object OneWay extends TestMessage
case object Die extends TestMessage
case object NotifySupervisorExit extends TestMessage
