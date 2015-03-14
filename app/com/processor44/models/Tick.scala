package com.processor44.models

import play.api.libs.json.Json

/**
 * Simple model - server timestamp capture
 */
case class Tick(ts: Long)
object Tick {
  implicit val jsonWriter = Json.writes[Tick]
  implicit val jsonReader = Json.reads[Tick] // Json.fromJson[Tick](jsval).asOpt
}
