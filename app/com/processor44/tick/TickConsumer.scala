package com.processor44.tick

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.ConfigFactory
import java.util.Properties
import kafka.consumer._
import kafka.message.MessageAndMetadata
import play.api.Logger
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
  case object Test
  val propsTickConsumerActor = Props[TickConsumerActor]

  // To broadcast what is consumed from kafka out to web clients
  // tickOut is Enumerator[JsValue], tickChannel is Concurrent.Channel[JsValue]
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
    case TickConsumer.Test => if (log.isInfoEnabled) log.info("TickConsumer.Test!")
    case TickConsumer.Consume => {
      if (log.isInfoEnabled) log.info("TickConsumerActor consuming...")
      val topicStreamMap = connector.createMessageStreams(Map(TickProducer.TOPIC -> 1))
      topicStreamMap.get(TickProducer.TOPIC) match {
        case None => log.error("TickConsumerActor NONE for Stream.  Can't Consume.")
        case Some(streamList) => {
          val kStream: KafkaStream[Array[Byte], Array[Byte]] = streamList(0)
          iterateStream(kStream)
        }
      }
    }
    case TickConsumer.Shutdown => {
      if (log.isInfoEnabled) log.info("TickConsumerActor shutting down...")
      connector.shutdown
    }
  }

  /**
   * while (iter.hasNext()) ... consumeAndPublishOne for each
   * @param kStream
   */
  def iterateStream(kStream: KafkaStream[Array[Byte], Array[Byte]]): Unit = {
    val iter: ConsumerIterator[Array[Byte], Array[Byte]] = kStream.iterator()
    while (iter.hasNext()) {
      consumeAndPublishOne(iter.next())
    }
  }

  /**
   *
   * @param mam
   * @return
   */
  def consumeAndPublishOne(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
    try {
      val k = getKeyAsString(mam, TickProducer.CHARSET)
      val m = new String(mam.message, TickProducer.CHARSET) // back to string json
      if (log.isDebugEnabled) log.debug("consumed [" + k + " " +  m + "] at partition " +
        mam.partition+ ", at offset " + mam.offset)
      TickConsumer.tickChannel.push(Json.parse(m)) // broadcast it
      true
    } catch {
      case t: Throwable => {
        Logger.error("consumeAndPublishOne ERROR ", t)
        false
      }
    }
  }

  /**
   * checks mam.key for null, default when empty is ""
   */
  def getKeyAsString(mam: MessageAndMetadata[Array[Byte], Array[Byte]], charsetName: String = TickProducer.CHARSET): String = {
    if (mam.key == null) ""
    else (new String(mam.key, charsetName))
  }

}
