package com.processor44

import akka.actor.ActorPath
import com.processor44.tick.TickConsumer
import play.api._
import play.api.libs.concurrent.Akka
import play.api.Play.current

/**
 * Created by markwilson on 3/13/15.
 */
object PkGlobal {

  var pathTickConsumer = "/user/TickConsumer"

  def onStart(): Unit = {
    Logger.debug("PkGlobal.onStart")
    val a = Akka.system.actorOf(TickConsumer.propsTickConsumerActor, "TickConsumer")
    //println(a.path)

    val testA = Akka.system.actorSelection(pathTickConsumer)
    testA ! TickConsumer.Test

    // a ! TickConsumer.Consume
  }

  def onStop(): Unit = {
    Logger.debug("PkGlobal.onStop")
    val a = Akka.system.actorSelection(pathTickConsumer)
    a ! TickConsumer.Test
    //a ! TickConsumer.Shutdown
    Akka.system.shutdown()
    //Akka.system.awaitTermination()
  }


}
