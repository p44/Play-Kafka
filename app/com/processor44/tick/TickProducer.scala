package com.processor44.tick

import com.processor44.models.Tick
import com.typesafe.config.ConfigFactory
import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import play.api.Logger
import play.api.libs.json.Json

/**
 * Produces messages to kafka topic defined by config "producer.topic.name.tick"
 */
object TickProducer {

  lazy val CONF = ConfigFactory.load
  lazy val TOPIC = CONF.getString("kafka.topic.name.tick")
  lazy val BROKER_LIST = CONF.getString("producer.metadata.broker.list")


  // http://kafka.apache.org/documentation.html#producerconfigs
  // "serializer.class" default is kafka.serializer.DefaultEncoder Array[Byte] to Array[Byte]
  val props = new Properties()
  props.put("metadata.broker.list", BROKER_LIST)
  props.put("request.required.acks", "1") // 1 is leader received
  props.put("producer.type", "sync")
  props.put("compression.codec", "none")
  props.put("message.send.max.retries", "3")

  val PRODUCER = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  /**
   * Converts to json and calls produce(message: String)
   * @param tick
   */
  def produce(tick: Tick): Unit = {
    val message = Json.stringify(Json.toJson(tick))
    produce(message)
  }
  /**
   * Calls PRODUCER.send(new KeyedMessage(TOPIC, message.getBytes("UTF8")))
   *
   * @param message assumes verified json
   */
  def produce(message: String): Unit = {
    Logger.debug("producing " + message)
    val km: KeyedMessage[AnyRef, AnyRef] = new KeyedMessage(TOPIC, message.getBytes("UTF8"))
    try {
      PRODUCER.send(km)
    } catch {
      case e: Exception => Logger.error("TickProducer - Failed to send " + message, e)
    }
  }

}
