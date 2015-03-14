import play.api.GlobalSettings

import com.processor44.PkGlobal

/** */
object Global extends GlobalSettings {

  override def onStart(app: play.api.Application) {
    println("Global.onStart")
    PkGlobal.onStart
  }

  override def onStop(app: play.api.Application) {
    println("Global.onStop")
    PkGlobal.onStop
  }

}
