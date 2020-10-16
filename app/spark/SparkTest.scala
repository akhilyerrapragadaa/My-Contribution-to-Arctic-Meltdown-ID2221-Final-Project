package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import org.apache.spark.sql.functions._

object SparkTest {

  def MethaneEmissions : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/methane.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"methane").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => (x(2) ,x(0)))

    sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    sam.take(10).foreach(println)

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }

   def NOEmissions : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
    import sparkS.implicits._

     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
     val session = cluster.connect()

     session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
     session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/NO.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"nitrous").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => (x(2) ,x(0)))

    sam.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    sam.take(10).foreach(println)

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