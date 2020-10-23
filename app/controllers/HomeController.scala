package controllers

import javax.inject.Inject
import play.api.mvc._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

// Spark
import services.DataProcessing

class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action { implicit request =>

  val plot = Vegas("Country Pop").
  withData(
    Seq(
      Map("country" -> "USA", "population" -> 314),
      Map("country" -> "UK", "population" -> 64),
      Map("country" -> "DK", "population" -> 80)
    )
  ).
  encodeX("country", Nom).
  encodeY("population", Quant).
  mark(Bar)

  Ok(plot.toJson)
  }

    //  Methane Emissions end point
  def CO2Emissions = Action { implicit request =>
  	val conf = DataProcessing.CO2Emissions
    Ok(views.html.test_args(s"$conf"))
  }

  //  Methane Emissions end point
  def Methane = Action { implicit request =>
  	val conf = DataProcessing.MethaneEmissions
    Ok(views.html.test_args(s"$conf"))
  }

  // NO Emissions end point
  def NOEmissions = Action { implicit request =>
  	val conf = DataProcessing.NOEmissions
    Ok(views.html.test_args(s"$conf"))
  }

  // PolarIce end point
  def PolarIce = Action { implicit request =>
  	val conf = DataProcessing.PolarIce
    Ok(views.html.test_args(s"$conf"))
  }

  // Temperature end point
  def Temperature = Action { implicit request =>
    val conf = DataProcessing.Temperature
    Ok(views.html.test_args(s"$conf"))
  }

}
