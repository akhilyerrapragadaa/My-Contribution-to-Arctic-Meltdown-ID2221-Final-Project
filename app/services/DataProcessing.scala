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

    // val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
   //  val session = cluster.connect()

   //  session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
   //  session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/co2-api.json"
    val df = sparkS.read.json(fileName)

    val content = df.select(explode($"co2").alias("new_batter"))

    val bat = content.select("new_batter.*")

    bat.printSchema()

    val sam = bat.rdd.map(x => ( x(2).toString.split('.')(0).toInt ,x(0).toString.split('.')(0).toInt ))

  //  sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    
    val dar = sam.reduceByKey(_+_)

    var agg = dar.map(x => (x._1, x._2/12)).sortBy(_._2)

    // Ppm to Metric tons per cubic Kmtr
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 ))

    inacc.take(50).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

 
  def MethaneEmissions : Int = {

    val sparkS = SparkSession.builder.master("local[2]").getOrCreate
    import sparkS.implicits._

    // val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
   //  val session = cluster.connect()

   //  session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
   //  session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/methane.json"
    val df = sparkS.read.json(fileName)

    val content = df.select(explode($"methane").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => ( x(2).toString.split('.')(0).toInt ,x(0).toString.split('.')(0).toInt ))

  //  sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    
    val dar = sam.reduceByKey(_+_)

    var agg = dar.map(x => (x._1, x._2/12)).sortBy(_._2)

    // Ppm to Metric tons per cubic Kmtr
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 ))

    inacc.take(50).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

   def NOEmissions : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
    import sparkS.implicits._

   //  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
   //  val session = cluster.connect()

  //   session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
   //  session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/NO.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"nitrous").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => ( x(2).toString.split('.')(0).toInt ,x(0).toString.split('.')(0).toInt ))

  //  sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    
    val dar = sam.reduceByKey(_+_)

    var agg = dar.map(x => (x._1, x._2/12)).sortBy(_._2)

    // Ppm to Metric tons per cubic Kmtr
    var calculation = agg.map(x =>( x._1, x._2 * 1.233))

    var oth = calculation.map(x =>( x._1, (x._2 * 1000000)/4047 ))

    var otherr = oth.map(x =>( x._1, x._2 / 1000 ))

    var inacc = otherr.map(x =>( x._1, x._2 / 27.2 ))

    inacc.take(50).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

  def PolarIce : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/PolarIce.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"result").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => (x(2) ,x(0)))

    sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    sam.take(10).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

  def Temperature : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/Temperature.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"result").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => (x(2) ,x(0)))

    sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    sam.take(10).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

}