package controllers

import com.processor44.models.{ViewModels, Msg, Tick}
import com.processor44.tick.{TickSimpleConsumer, TickConsumer, TickProducer}
import play.api._
import play.api.libs.EventSource
import play.api.libs.iteratee.{Concurrent, Enumeratee}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object Application extends Controller {

  def index = Action {
    Ok(views.html.index())
  }

  /**
   * Uses server timestamp to create a tick obj then produces it to kafka
   * @return
   */
  def putGenTick = Action.async { request =>
    // Send it to Kafka
    TickProducer.produce(Tick(System.currentTimeMillis())).map { r =>
      r match {
        case false => InternalServerError(ViewModels.MSG_ERROR_JSON)
        case true => Ok(ViewModels.MSG_SUCCESS_JSON)
      }
    }
  }

  def getLastOffset = Action.async { request =>
    Future {
      TickSimpleConsumer.getLastOffset(TickProducer.TOPIC, TickSimpleConsumer.PARTITION_DEF) match {
        case None => InternalServerError(ViewModels.MSG_ERROR_JSON)
        case Some(offset) => Ok(Json.prettyPrint(Json.toJson[Msg](Msg("Last Offset: " + offset))))
      }
    }
  }

  def getLastMessages(count: Int) = Action.async { request =>
    Future {
      val r: Map[Long,(String, String)] =
        TickSimpleConsumer.fetchLastMessages(TickProducer.TOPIC, TickSimpleConsumer.PARTITION_DEF, count)
      r.isEmpty match {
        case true => Ok(Json.prettyPrint(Json.toJson[Msg](Msg("Empty Results"))))
        case false => Ok(Json.prettyPrint(Json.toJson[Msg](Msg("Last Offset: " + r.mkString(" ")))))
      }
    }
  }

  def getResetOffset(back: Int) = Action.async { request =>
    Future {
      TickSimpleConsumer.getLastOffset(TickProducer.TOPIC, TickSimpleConsumer.PARTITION_DEF) match {
        case None => InternalServerError(ViewModels.MSG_ERROR_JSON)
        case Some(offset) => {
          val resetTo: Long = calcResetBack(offset, back)
          TickSimpleConsumer.resetOffset(TickProducer.TOPIC, TickSimpleConsumer.PARTITION_DEF, resetTo) match {
            case None => InternalServerError(ViewModels.MSG_ERROR_JSON)
            case Some(newOffset) => {
              if (Logger.isDebugEnabled) Logger.debug("getResetOffset last offset " + offset + " newOffset " + newOffset)
              Ok(Json.prettyPrint(Json.toJson[Msg](Msg("Last Offset: " + offset+ " newOffset " + newOffset))))
            }
          }
        }
      }
    }
  }

  def calcResetBack(offset: Long, back: Int): Long = {
    val nOffset = offset - back
    nOffset < 0 match {
      case true => 0L
      case false => nOffset
    }
  }

  // Tick Feed - The Tick consumer will put to the tick chanel json pulled from kafka

  /** Enumeratee for detecting disconnect of the stream */
  def logDisconnect(addr: String): Enumeratee[JsValue, JsValue] = {
    Enumeratee.onIterateeDone { () =>
      Logger.info(addr + " - tickOut disconnected")
    }
  }

  /** Controller action serving activity for tick json consumed from kafka */
  def tickFeed = Action { req =>
    Logger.info("FEED tick - " + req.remoteAddress + " - tick connected")
    Ok.chunked(TickConsumer.tickOut
      &> Concurrent.buffer(100)
      &> logDisconnect(req.remoteAddress)
      &> EventSource()).as("text/event-stream")
    // &>  Compose this Enumerator with an Enumeratee. Alias for through
  }

}