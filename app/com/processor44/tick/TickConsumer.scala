package com.processor44.tick

import akka.actor.{Props, ActorLogging, Actor}
import com.processor44.models.Tick
import com.typesafe.config.ConfigFactory
import java.util.Properties
import play.api.Logger

import kafka.consumer.{ConsumerConnector, Consumer, ConsumerConfig, KafkaStream}
import play.api.libs.iteratee.Concurrent
import play.api.libs.json.{Json, JsValue}

/**
 * Consumes messages from kafka topic defined by config "producer.topic.name.tick"
 */
object TickConsumer {

  lazy val CONF = ConfigFactory.load
  lazy val ZOOKEEPER_CON = CONF.getString("zookeeper.connect")
  lazy val GROUP_ID = "1"
  lazy val CONSUMER_CONFIG = new ConsumerConfig(buildConsumerProps)

  def buildConsumerProps: Properties = {
    val p = new Properties()
    p.put("group.id", GROUP_ID)
    p.put("zookeeper.connect", ZOOKEEPER_CON)
    p.put("auto.commit.enable", "true")  // If true, periodically commit to ZooKeeper the offset of messages already
    // fetched by the consumer. This committed offset will be used when the process fails as the position from which the
    // new consumer will begin.  Default 60 seconds.
    p
  }

  // Consumer actor
  case object Consume
  case object Shutdown
  val propsTickConsumerActor = Props[TickConsumerActor]

  // To broadcast what is consumed from kafka out to web clients
  val (tickOut, tickChannel) = Concurrent.broadcast[JsValue]

}

/**
 * Consumes from kafka topic defined by config "producer.topic.name.tick"
 * Feeds out to another stream
 */
class TickConsumerActor extends Actor with ActorLogging {

  val connector: ConsumerConnector = Consumer.create(TickConsumer.CONSUMER_CONFIG)

  /** */
  def receive = {
    case TickConsumer.Consume => {
      log.info("TickConsumerActor consuming...")
      val topicStreamMap = connector.createMessageStreams(Map(TickProducer.TOPIC -> 1))
      val kStream: KafkaStream[Array[Byte], Array[Byte]] = topicStreamMap.get(TickProducer.TOPIC).get(0)
      // stream away...
      for (mAndM <- kStream) {
        try {
          val m = new String(mAndM.message, "UTF-8")
          log.debug("consumed " + m + " at offset " + mAndM.offset)

          // broadcast it
          val jsv = Json.parse(m)
          TickConsumer.tickChannel.push(jsv)
        } catch {
          case t: Throwable => Logger.error("TickConsumerActor ERROR " + t)
        }
      }
    }
    case TickConsumer.Shutdown => {
      log.info("TickConsumerActor shutting down...")
      connector.shutdown
    }
  }
}
