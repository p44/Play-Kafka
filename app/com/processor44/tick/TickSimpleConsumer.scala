package com.processor44.tick

import kafka.api.PartitionOffsetRequestInfo
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.{OffsetRequest, OffsetResponse}
import kafka.message.{MessageAndOffset, Message}
import play.api.Logger
import scala.util.control.NonFatal

/**
 * // https://people.apache.org/~junrao/kafka-0.8.2.0-candidate2/scaladoc/index.html#kafka.javaapi.consumer.SimpleConsumer
 */
object TickSimpleConsumer {

  val PARTITION_DEF = 0
  val TO_DEF = 100000 // timeout
  val BUFF = 64 * 1024 // buffer size

  def newSimpleConsumer(host: String, port: Int, clientId: String): SimpleConsumer = {
    new SimpleConsumer(host, port, TO_DEF, BUFF, clientId)
  }

  /**
   * Usage:
   * getOffset(host, port, clientId, kafkaTopic, partition, kafka.api.OffsetRequest.LatestTime)
   *
   * @param host use TickProducer.BROKER_LIST_PARSED_HP(i)._1
   * @param port use TickProducer.BROKER_LIST_PARSED_HP(i)._2
   * @param clientId use TickConsumer.GROUP_ID
   * @param topic the topic from the config file def "tick"
   * @param partition for now use PARTITION_DEF; 0
   * @param whichTime either kafka.api.OffsetRequest.{LatestTime, EarliestTime}
   * @return
   */
  def getOffset(host: String, port: Int, clientId: String, topic: String, partition: Int, whichTime: Long): Option[OffsetResponse] = {
    val hpString = host + ":" + port
    withSimpleConsumer(host, port, clientId) { simpCo =>
      val response = getOffset(simpCo, topic, partition, whichTime, clientId)
      if (Logger.isInfoEnabled) Logger.info("Good response in getOffset --> whichTime (" + whichTime + ") offset response [" + hpString + "]: " + response)
      return Some(response)
    }
    None
  }

  def getOffset(simpCo: SimpleConsumer, topic: String, partition: Int, whichTime: Long, clientId: String): OffsetResponse = {
    val oReq = buildOffsetRequest(topic, partition, whichTime, clientId)
    if (Logger.isDebugEnabled) {
      Logger.debug("OffsetRequest: " + oReq.toString)
    }
    simpCo.getOffsetsBefore(oReq)
  }

  def buildOffsetRequest(topic: String, partition: Int, whichTime: Long, clientId: String): OffsetRequest = {
    val partitionOffset = new PartitionOffsetRequestInfo(whichTime, 1) // 1 is maxNumOffsets
    val requestInfo = new java.util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]
    requestInfo.put((new TopicAndPartition(topic, partition)), partitionOffset)
    new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientId)
  }

  /**
   * Usage:
   * TickSimpleConsumer.getLastOffset(TickProducer.TOPIC, TickSimpleConsumer.PARTITION_DEF)
   *
   * Calls getOffset(host, port, clientId, kafkaTopic, partition, kafka.api.OffsetRequest.LatestTime)
   * with defaults: first host port found in Broker_List and TickConsumer.GROUP_ID as client id
   *
   * @param topic
   * @param partition
   * @return
   */
  def getLastOffset(topic: String, partition: Int): Option[Long] = {
    val host = TickProducer.BROKER_LIST_PARSED_HP(0)._1
    val port = TickProducer.BROKER_LIST_PARSED_HP(0)._2
    val clientId = TickConsumer.GROUP_ID
    val oResponse = getOffset(host, port, clientId, topic, partition, kafka.api.OffsetRequest.LatestTime)
    oResponse match {
      case None => Logger.warn("None last offset"); None
      case Some(res) =>
        res.hasError match {
          case true => Logger.error(res.toString); None
          case false => firstOffsetInArray(res.offsets(topic, partition))
        }
    }
  }

  def firstOffsetInArray(offsets: Array[Long]): Option[Long] = {
    if (offsets.size > 0) Some(offsets(0))
    else None
  }

  /**
   * Fetch Messages
   *
   * @param simpCo the simple consumer
   * @param clientId the client id a.k.a. client group id
   * @param topic the topic name
   * @param partition the partition
   * @param readOffset the offset to read from - starting point
   * @param fetchSize the number of messages to fetch
   */
  def fetchMessages(simpCo: SimpleConsumer, clientId: String, topic: String, partition: Int, readOffset: Long, fetchSize: Int): kafka.javaapi.FetchResponse = {
    // addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int)
    val req = (new kafka.api.FetchRequestBuilder()).clientId(clientId).addFetch(topic, partition, readOffset, fetchSize).build()
    simpCo.fetch(req)
  }

  /**
   * TODO iterator does not have next as expected - needs attention
   *
   * @param res
   * @param topic
   * @param partition
   * @return a map of valid messages as [offset -> (key, message)]
   */
  def doFetchResponse(res: kafka.javaapi.FetchResponse, topic: String, partition: Int): Map[Long,(String, String)] = {
    if (res.hasError) {
      Map.empty[Long,(String, String)]
    } else {
      val bbms: ByteBufferMessageSet = res.messageSet(topic, partition)
      if (Logger.isDebugEnabled) Logger.debug("bbms " + bbms.sizeInBytes)
      val convertedMessages = scala.collection.mutable.Map[Long,(String, String)]()
      val iter = bbms.iterator()
      var numRead = 0
      while (iter.hasNext) {
        println("XXX")
        val maos: MessageAndOffset = iter.next
        val currentOffset = maos.offset
        val readOffset = maos.nextOffset
        if (Logger.isDebugEnabled) Logger.debug ("currentOffset " + currentOffset + " readOffset " + readOffset)
        val message = maos.message
        if (Logger.isDebugEnabled) Logger.debug ("consumed " + message.toString + " at " + maos.offset)
        convertMessage(maos.offset, message).map(kvTuple => convertedMessages += (maos.offset -> kvTuple))
        numRead += 1
      }
      println("numRead " + numRead)
      if (numRead == 0) { // kafka recommended additional check
        try { Thread.sleep(1000) } catch {
          case e: InterruptedException => // do nothing
        }
      }
      convertedMessages.toMap[Long,(String, String)]
    }
  }

  def convertMessage(offset: Long, message: Message): Option[(String, String)] = {
    message.isValid match {
      case false => {
        if (Logger.isDebugEnabled) Logger.debug("invalid message at offset " + offset)
        None
      }
      case true => {
        message.hasKey match {
          case false => {
            Some(("", convertPayload(message)))
          }
          case true => {
            val k = TickConsumer.getKeyAsString(message.key.array(), TickProducer.CHARSET)
            Some((k, convertPayload(message)))
          }
        }
      }
    }
  }

  def convertPayload(message: Message): String = {
    message.isNull match {
      case false => ""
      case true => (new String(message.payload.array(), TickProducer.CHARSET))
    }
  }

  /**
   * Calls fetchMessages withSimpleConsumer
   *
   * @param topic
   * @param partition
   * @param fetchSize
   * @return
   */
  def fetchLastMessages(topic: String, partition: Int, fetchSize: Int): Map[Long,(String, String)] = {
    val host = TickProducer.BROKER_LIST_PARSED_HP(0)._1
    val port = TickProducer.BROKER_LIST_PARSED_HP(0)._2
    val clientId = TickConsumer.GROUP_ID
    getLastOffset(topic, partition).map { lastOffset =>
      val startingOffset = lastOffset - fetchSize - 1L
      if (Logger.isDebugEnabled) Logger.debug("fetchLastMessages startingOffset " + startingOffset)
      val hpString = host + ":" + port
      withSimpleConsumer(host, port, clientId) { simpCo =>
        val fetchResponse = fetchMessages(simpCo, clientId, topic, partition, startingOffset, fetchSize)
        if (Logger.isInfoEnabled) Logger.info("Good response in fetchLastMessages --> fetchSize (" + fetchSize +
          ") offset response [" + hpString + "]: error? " + fetchResponse.hasError)
        val x = doFetchResponse(fetchResponse, topic, partition)
        return x
      }
    }
    Map.empty[Long,(String, String)]
  }

  /**
   * NOTE:  assumes a single broker single partition topic, first in TickProducer.BROKER_LIST_PARSED_HP
   * Uses these:
   * val host = TickProducer.BROKER_LIST_PARSED_HP(0)._1
   * val port = TickProducer.BROKER_LIST_PARSED_HP(0)._2
   * val clientId = TickConsumer.GROUP_ID
   *
   * Usage:
   * TickSimpleConsumer.resetOffset(TickProducer.TOPIC, TickSimpleConsumer.PARTITION_DEF, resetTo)
   *
   * @param topic
   * @param partition
   * @param offset
   * @return
   */
  def resetOffset(topic: String, partition: Int, offset: Long): Option[Long] = {
    val host = TickProducer.BROKER_LIST_PARSED_HP(0)._1
    val port = TickProducer.BROKER_LIST_PARSED_HP(0)._2
    val clientId = TickConsumer.GROUP_ID
    resetOffset(host, port, clientId, (new TopicAndPartition(topic, partition)), OffsetAndMetadata(offset))
  }

  /**
   * Builds an OffsetCommitRequest
   * Calls simpCo.CommitOffsets() using withSimpleConsumer
   *
   * @param host
   * @param port
   * @param clientId
   * @param tp
   * @param om
   * @return
   */
  def resetOffset(host: String, port: Int, clientId: String, tp: TopicAndPartition, om: OffsetAndMetadata): Option[Long] = {
    var result = Map[TopicAndPartition, Short]()
    val curVer = kafka.api.OffsetRequest.CurrentVersion
    val tpOmMap = new java.util.HashMap[TopicAndPartition,OffsetAndMetadata]()
    tpOmMap.put(tp, om)
    val req = new kafka.javaapi.OffsetCommitRequest(clientId, tpOmMap, 0, clientId, curVer)
    val hpString = host + ":" + port
    withSimpleConsumer(host, port, clientId) { simpCo =>
      val resp: kafka.javaapi.OffsetCommitResponse = simpCo.commitOffsets(req)
      resp.hasError match {
        case true => {
          Logger.error("resetOffset ERROR --> tp (" + tp + ")  hp [" + hpString + "] code: " + resp.errorCode(tp))
          return None
        }
        case false => {
          if (Logger.isDebugEnabled) Logger.debug("resetOffset SUCCESS --> tp (" + tp + ")  hp [" + hpString + "] new offset " + om.offset)
          return Some(om.offset)
        }
      }
    }
    None
  }

  /**
   * Function for calling functions on SimpleConsumer with exception handling.
   *
   * @param host
   * @param port
   * @param clientId
   * @param func
   * @return
   */
  private def withSimpleConsumer(host: String, port: Int, clientId: String)(func: SimpleConsumer => Any): Unit = {
    var simpCo: SimpleConsumer = null
    try {
      simpCo = newSimpleConsumer(host, port, clientId)
      func.apply(simpCo)
    } catch {
      case NonFatal(e) => {
        val hpString = host + ":" + port
        Logger.error("Error in withSimpleConsumer --> clientId (" + clientId + ")  hp [" + hpString + "]", e)
      }
    } finally {
      if (simpCo != null) {
        if (Logger.isDebugEnabled) Logger.debug("simpCo.close()")
        simpCo.close()
      }
    }
  }
}
