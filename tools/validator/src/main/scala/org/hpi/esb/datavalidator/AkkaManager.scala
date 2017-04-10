package org.hpi.esb.datavalidator

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object AkkaManager {

  lazy val system: ActorSystem = ActorSystem("ESBValidator")
  val materializer: ActorMaterializer = ActorMaterializer()(system)

  def terminate(): Unit = system.terminate()
}
