package services

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}

object DataProcessing {


   def CO2Emissions : Int = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS environmental_calculations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS environmental_calculations.co2 (year int PRIMARY KEY, tonnes float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/co2-api.json"
    val df = sparkS.read.json(fileName)

    val content = df.select(explode($"co2").alias("co2_data"))

    val bat = content.select("co2_data.*")

    bat.printSchema()

     val sam = bat.rdd.map(x => ( x(4).toString.toInt ,x(0).toString.toDouble ))
    
    val dar = sam.reduceByKey(_+_)

    var agg = dar.map(x => (x._1, x._2/365)).sortBy(_._2)

    // Ppm to Metric tons per cubic Kmtr
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 ))

     inacc.saveToCassandra("environmental_calculations", "co2", SomeColumns("year", "tonnes"))

    inacc.take(50).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

 
  def MethaneEmissions : Int = {

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

    // Ppm to Metric tons per cubic Kmtr
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 )).sortBy(_._2)


    inacc.take(50).foreach(println)

    inacc.saveToCassandra("environmental_calculations", "methane", SomeColumns("year", "tonnes"))

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

   def NOEmissions : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
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

    // Ppm to Metric tons per cubic Kmtr
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 )).sortBy(_._2)

    inacc.take(50).foreach(println)

    inacc.saveToCassandra("environmental_calculations", "noemissions", SomeColumns("year", "tonnes"))

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

  def PolarIce : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
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

    val sam = bat.rdd.map(x => (x(2) ,x(0)))

    val sam2 = bat.rdd.map(x => (x(2) ,x(1)))

    sam.saveToCassandra("environmental_calculations", "polaricevalues", SomeColumns("year", "area"))

    sam2.saveToCassandra("environmental_calculations", "polaricevalues", SomeColumns("year", "extent" ))

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

  def Temperature : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
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
    
    agg.saveToCassandra("environmental_calculations", "temperaturechanges", SomeColumns("year", "temperature"))

    agg.take(150).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

}