package com.processor44.tick

import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.javaapi.{OffsetRequest, OffsetResponse}
import kafka.javaapi.consumer.SimpleConsumer
import play.api.Logger

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
    var simpCo: SimpleConsumer = null
    val hpString = host + ":" + port
    try {
      simpCo = newSimpleConsumer(host, port, clientId)
      val response = getOffset(simpCo, topic, partition, whichTime, clientId)
      if (Logger.isInfoEnabled) Logger.info("Good response in getOffset --> whichTime (" + whichTime + ") offset response [" + hpString + "]: " + response)
      Some(response)
    }
    catch {
      case e: Exception => {
        Logger.error("Error in getOffset --> whichTime (" + whichTime + ") host-port [" + hpString + "]", e)
        None
      }
    }
    finally {
      if (simpCo != null) {
        if (Logger.isDebugEnabled) Logger.debug("simpCo.close()")
        simpCo.close()
      }
    }
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
   * @param kafkaTopic
   * @param partition
   * @return
   */
  def getLastOffset(kafkaTopic: String, partition: Int): Option[Long] = {
    val host = TickProducer.BROKER_LIST_PARSED_HP(0)._1
    val port = TickProducer.BROKER_LIST_PARSED_HP(0)._2
    val clientId = TickConsumer.GROUP_ID
    val oResponse = getOffset(host, port, clientId, kafkaTopic, partition, kafka.api.OffsetRequest.LatestTime)
    oResponse match {
      case None => Logger.warn("None last offset"); None
      case Some(res) =>
        res.hasError match {
          case true => Logger.error(res.toString); None
          case false => firstOffsetInArray(res.offsets(kafkaTopic, partition))
        }
    }
  }

  def firstOffsetInArray(offsets: Array[Long]): Option[Long] = {
    if (offsets.size > 0) Some(offsets(0))
    else None
  }
}
