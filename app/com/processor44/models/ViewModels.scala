package com.processor44.models

import play.api.libs.json.Json

case class Msg(msg: String)
object Msg {
  implicit val jsonWriter = Json.writes[Msg]
  implicit val jsonReader = Json.reads[Msg]
}

/** */
object ViewModels {
  lazy val MSG_SUCCESS: Msg = Msg("Success")
  lazy val MSG_SUCCESS_JSON = Json.prettyPrint(Json.toJson[Msg](MSG_SUCCESS))
  lazy val MSG_BAD_JSON: Msg = Msg("Could not resolve json for Tick.  Check Content-Type and json format.")
  lazy val MSG_BAD_JSON_JSON = Json.prettyPrint(Json.toJson[Msg](MSG_BAD_JSON))
  lazy val MSG_ERROR: Msg = Msg("An error occured")
  lazy val MSG_ERROR_JSON = Json.prettyPrint(Json.toJson[Msg](MSG_ERROR))
}
