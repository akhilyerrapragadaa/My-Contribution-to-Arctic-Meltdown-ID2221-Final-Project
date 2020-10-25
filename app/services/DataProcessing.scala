package services

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

import com.cloudera.sparkts.models.ARIMA;
import com.cloudera.sparkts.models.ARIMAModel;

object DataProcessing {

   def CO2Emissions : String = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS environmental_calculations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.co2 (year int PRIMARY KEY, tonnes float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/co2-api.json"
    val dataframe = sparkS.read.json(fileName)

    val explodedDataframe = dataframe.select(explode($"co2").alias("co2_data"))

    val selectedDataFrame = explodedDataframe.select("co2_data.*")

    selectedDataFrame.printSchema()

     val inputRDD = selectedDataFrame.rdd.map(x => ( x(4).toString.toInt ,x(0).toString.toDouble ))
    
    val reducedRDD = inputRDD.reduceByKey(_+_)

    var averagedRDD = reducedRDD.map(x => (x._1, x._2/365)).sortBy(_._2)

    // Ppm to Kg per acre foot
    var ppmTokgRDD = averagedRDD.map(x =>( x._1, x._2 * 1.233))

    // Kg per acre to Kg per sqkm conversion
    var acreTosqkmRDD = ppmTokgRDD.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    // Kg per sqkm to tons per sqkm
    var kgToTonsRDD = acreTosqkmRDD.map(x =>( x._1, x._2 / 1000 ))

    //Tons per sqkm / population per sqkm
    var finalRDD = kgToTonsRDD.map(x =>( x._1, x._2 / 27.2 )).sortBy(_._2)

    finalRDD.saveToCassandra("environmental_calculations", "co2", SomeColumns("year", "tonnes"))

     var modifiedRDD = finalRDD.filter(_._1 != 2020)

    val dffplot1 = sparkS.createDataFrame(modifiedRDD).toDF("id", "vals")

    dffplot1.printSchema();

     val plot1 = Vegas("Co2 Emissions").withDataFrame(dffplot1).
     mark(Line).
     encodeX("id", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("vals", Quant, axis = Axis(title="Co2 Emission in Tons Per Person, Per Year")).
     configCell(width=700, height=500)

  
    modifiedRDD.take(50).foreach(println)

    val onlyValuesRDD = modifiedRDD.map(_._2.toDouble).collect.toArray

    val dense = Vectors.dense(onlyValuesRDD)

    val arimaModel = ARIMA.autoFit(dense)

    println("Coefficients: " + arimaModel.coefficients.mkString(","))

    val forecast = arimaModel.forecast(dense,20)

    session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.predictioncalculations (year int PRIMARY KEY, co2predicted float, methanepredicted float, nopredicted float);")
   
    var myList = Array(2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035,2036,2037,2038,2039)

    val largeRDD = sparkS.sparkContext.parallelize(forecast.toArray)

    val smallRDD = sparkS.sparkContext.parallelize(myList)

    val befcombin = sparkS.sparkContext.parallelize(largeRDD.top(20).reverse); 

    val combin = smallRDD.zip(befcombin)

    val dffplot = sparkS.createDataFrame(combin).toDF("id", "vals")

    dffplot.printSchema();

     val plot = Vegas("Co2 Emissions").withDataFrame(dffplot).
     mark(Line).
     encodeX("id", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("vals", Quant, axis = Axis(title="Co2 Emission in Tons Per Person, Per Year")).
     configCell(width=700, height=500)

    combin.take(50).foreach(println)

    combin.saveToCassandra("environmental_calculations", "predictioncalculations", SomeColumns("year", "co2predicted") )

    combin.take(50).foreach(println)

    println("Forecast of next 20 observations: " + forecast.toArray.mkString(","))

    return plot1.toJson
  }

 
  def MethaneEmissions : String = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS environmental_calculations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.methane (year int PRIMARY KEY, tonnes float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/methane.json"
    val df = sparkS.read.json(fileName)

    val content = df.select(explode($"methane").alias("methane_data"))

    val bat = content.select("methane_data.*")

    val sam = bat.rdd.map(x => ( x(2).toString.split('.')(0).toInt ,x(0).toString.split('.')(0).toInt ))
    
    val dar = sam.reduceByKey(_+_)

    var agg = dar.map(x => (x._1, x._2/12)).sortBy(_._2)

    // Ppm to Kg per acre foot
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    // Kg per acre to Kg per sqkm conversion
    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    // Kg per sqkm to tons per sqkm
    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    //Tons per sqkm / population per sqkm
    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 )).sortBy(_._2)


    inacc.saveToCassandra("environmental_calculations", "methane", SomeColumns("year", "tonnes"))

    var modific = inacc.filter(_._1 != 2020)

    modific.take(50).foreach(println)


     val dffplot1 = sparkS.createDataFrame(modific).toDF("id", "vals")

    dffplot1.printSchema();

     val plot1 = Vegas("Methane Emissions").withDataFrame(dffplot1).
     mark(Line).
     encodeX("id", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("vals", Quant, axis = Axis(title="Methane Emission in Tons Per Person, Per Year")).
     configCell(width=700, height=500)

    val deom = modific.map(_._2.toDouble).collect.toArray

    val ts = Vectors.dense(deom)

    val arimaModel1 = ARIMA.autoFit(ts)

    println("Coefficients: " + arimaModel1.coefficients.mkString(","))

    val forecast1 = arimaModel1.forecast(ts,20)

    session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.predictioncalculations (year int PRIMARY KEY, co2predicted float, methanepredicted float, nopredicted float);")
   
    var myList = Array(2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035,2036,2037,2038,2039)

    val largeRDD = sparkS.sparkContext.parallelize(forecast1.toArray)

    val smallRDD = sparkS.sparkContext.parallelize(myList);

    val befcombin = sparkS.sparkContext.parallelize(largeRDD.top(20).reverse); 

    val combin = smallRDD.zip(befcombin)

    val dffplot = sparkS.createDataFrame(combin).toDF("id", "vals")

    dffplot.printSchema();

     val plot = Vegas("Methane Emissions").withDataFrame(dffplot).
     mark(Line).
     encodeX("id", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("vals", Quant, axis=Axis(title="Methane Emission in Tons Per Person, Per Year")).
     configCell(width=700, height=500)

    combin.take(50).foreach(println)

    combin.saveToCassandra("environmental_calculations", "predictioncalculations", SomeColumns("year", "methanepredicted") )

    println("Forecast of next 20 observations: " + forecast1.toArray.mkString(","))
 
    return plot1.toJson

  }

   def NOEmissions : String = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS environmental_calculations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.noemissions (year int PRIMARY KEY, tonnes float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/NO.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"nitrous").alias("nitrous_oxide_data"))

    val bat = content.select("nitrous_oxide_data.*")

    val sam = bat.rdd.map(x => ( x(2).toString.split('.')(0).toInt ,x(0).toString.split('.')(0).toInt ))
    
    val dar = sam.reduceByKey(_+_)

    var agg = dar.map(x => (x._1, x._2/12)).sortBy(_._2)

   // Ppm to Kg per acre foot
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    // Kg per acre to Kg per sqkm conversion
    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    // Kg per sqkm to tons per sqkm
    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    //Tons per sqkm / population per sqkm
    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 )).sortBy(_._2)

    inacc.saveToCassandra("environmental_calculations", "noemissions", SomeColumns("year", "tonnes"))

    var modific = inacc.filter(_._1 != 2020)

    modific.take(50).foreach(println)

      val dffplot1 = sparkS.createDataFrame(modific).toDF("id", "vals")

      dffplot1.printSchema();

     val plot1 = Vegas("NO Emissions").withDataFrame(dffplot1).
     mark(Line).
     encodeX("id", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("vals", Quant, axis = Axis(title="NO Emission in Tons Per Person, Per Year")).
     configCell(width=700, height=500)

    val deom = modific.map(_._2.toDouble).collect.toArray

    val ts = Vectors.dense(deom)

    val arimaModel1 = ARIMA.autoFit(ts)

    println("Coefficients: " + arimaModel1.coefficients.mkString(","))

    val forecast1 = arimaModel1.forecast(ts,20)

    session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.predictioncalculations (year int PRIMARY KEY, co2predicted float, methanepredicted float, nopredicted float);")
   
    var myList = Array(2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035,2036,2037,2038,2039)

    val largeRDD = sparkS.sparkContext.parallelize(forecast1.toArray)

    val smallRDD = sparkS.sparkContext.parallelize(myList);

    val befcombin = sparkS.sparkContext.parallelize(largeRDD.top(20).reverse); 

    val combin = smallRDD.zip(befcombin)

    val dffplot = sparkS.createDataFrame(combin).toDF("id", "vals")

    dffplot.printSchema();

     val plot = Vegas("NO Emissions").withDataFrame(dffplot).
     mark(Line).
     encodeX("id", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("vals", Quant, axis=Axis(title="NO Emission in Tons Per Person, Per Year")).
     configCell(width=700, height=500)

    combin.take(50).foreach(println)

    combin.saveToCassandra("environmental_calculations", "predictioncalculations", SomeColumns("year", "nopredicted") )

    println("Forecast of next 20 observations: " + forecast1.toArray.mkString(","))
 
    return plot1.toJson

  }

  def PolarIce : String = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS environmental_calculations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.polaricevalues (year int PRIMARY KEY, area float, extent float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/PolarIce.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"result").alias("polarice_data"))

    val bat = content.select("polarice_data.*")

    bat.printSchema();

    val sam = bat.rdd.map(x => (x(2).toString.toInt ,x(0).toString.toDouble ))

     sam.take(50).foreach(println)

    val dffplot = sparkS.createDataFrame(sam).toDF("year", "area")

    dffplot.printSchema();

     val plot = Vegas("Area Remaining").withDataFrame(dffplot).
     mark(Line).
     encodeX("year", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("area", Quant, axis=Axis(title="Area Remaining in Million Sqkm")).
     configCell(width=700, height=500)

    val sam2 = bat.rdd.map(x => (x(2) ,x(1) ))

     sam2.take(50).foreach(println)

    sam.saveToCassandra("environmental_calculations", "polaricevalues", SomeColumns("year", "area"))

    sam2.saveToCassandra("environmental_calculations", "polaricevalues", SomeColumns("year", "extent" ))

     return plot.toJson

  }

  def Temperature : String = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS environmental_calculations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.temperaturechanges (year int PRIMARY KEY, temperature float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/Temperature.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"result").alias("temperature_data"))

    val bat = content.select("temperature_data.*")

    val sam = bat.rdd.map(x => (x(2).toString.split('.')(0).toInt ,x(0).toString.toDouble ))

     bat.printSchema();

     val dar = sam.reduceByKey(_+_)

     var agg = dar.map(x => (x._1, x._2/12)).sortBy(_._1)

     val dffplot = sparkS.createDataFrame(agg).toDF("year", "area")

     dffplot.printSchema();

     val plot = Vegas("Area Remaining").withDataFrame(dffplot).
     mark(Line).
     encodeX("year", Quant, scale = Scale(zero = false), axis=Axis(title="Year") ).
     encodeY("area", Quant, axis=Axis(title="Arctic Summer Temperatures")).
     configCell(width=700, height=500)

    agg.saveToCassandra("environmental_calculations", "temperaturechanges", SomeColumns("year", "temperature"))

    agg.take(150).foreach(println)

     return plot.toJson

  }

}