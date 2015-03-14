package controllers

import com.processor44.models.{ViewModels, Msg, Tick}
import com.processor44.tick.{TickConsumer, TickProducer}
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
   *
   * @return
   */
  def postTick = Action.async { request =>
    Future {
      val ojsv = request.body.asJson
      val oTick = ojsv.flatMap(jsv => jsv.validate[Tick].asOpt)
      Logger.debug("postTick " + oTick)
      oTick match {
        case None => BadRequest(ViewModels.MSG_BAD_JSON_JSON)
        case Some(tick) => {
          TickProducer.produce(Json.stringify(ojsv.get)) // Send it to Kafka
          Ok(ViewModels.MSG_SUCCESS_JSON)
        }
      }
    }
  }

  /**
   * Uses server timestamp to create a tick obj then produces it to kafka
   * @return
   */
  def putGenTick = Action.async { request =>
    Future {
      TickProducer.produce(Tick(System.currentTimeMillis()))
      Ok(ViewModels.MSG_SUCCESS_JSON)
    }
  }

  // Tick Feed

  /** Enumeratee for detecting disconnect of the stream */
  def connDeathLog(addr: String): Enumeratee[JsValue, JsValue] = {
    Enumeratee.onIterateeDone { () =>
      Logger.info(addr + " - tickOut disconnected")
    }
  }

  /** Controller action serving activity for tick json consumed from kafka */
  def tickFeed = Action { req =>
    Logger.info("FEED tick - " + req.remoteAddress + " - tick connected")
    Ok.chunked(TickConsumer.tickOut
      &> Concurrent.buffer(100)
      &> connDeathLog(req.remoteAddress)
      &> EventSource()).as("text/event-stream")
    // &>  Compose this Enumerator with an Enumeratee. Alias for through
  }

}