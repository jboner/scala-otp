
/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.behavior

import org.scalatest._

class AllSuite extends SuperSuite(
  List(
    new SupervisorSuite,
    new SupervisorStateSuite,
    new GenericServerSuite,
    new GenericServerContainerSuite
  )
)


