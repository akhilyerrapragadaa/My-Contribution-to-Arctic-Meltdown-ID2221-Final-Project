package controllers

import javax.inject.Inject
import play.api.mvc._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

// Spark
import services.DataProcessing

class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action { implicit request =>
  Ok(views.html.index())
  }

    //  Methane Emissions end point
  def CO2Emissions = Action { implicit request =>
  	val sum = DataProcessing.CO2Emissions
    Ok(views.html.test_args(s"A call to Spark, with result: $sum"))
  }

  //  Methane Emissions end point
  def Methane = Action { implicit request =>
  	val sum = DataProcessing.MethaneEmissions
    Ok(views.html.test_args(s"A call to Spark, with result: $sum"))
  }

  // NO Emissions end point
  def NOEmissions = Action { implicit request =>
  	val sum = DataProcessing.NOEmissions
    Ok(views.html.test_args(s"A call to Spark, with result: $sum"))
  }

  // PolarIce end point
  def PolarIce = Action { implicit request =>
  	val sum = DataProcessing.PolarIce
    Ok(views.html.test_args(s"A call to Spark, with result: $sum"))
  }

  // Temperature end point
  def Temperature = Action { implicit request =>
  	val sum = DataProcessing.Temperature
    Ok(views.html.test_args(s"A call to Spark, with result: $sum"))
  }

}
