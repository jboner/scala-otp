
/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import org.scalatest._

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class AllSuite extends SuperSuite(
  new InMemoryCustomPKSuite ::
  new InMemoryGeneratedPKSuite ::
  new TreapSuite ::
  Nil
)


