package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkTest {

  def Example : Int = {

    val sparkS = SparkSession.builder.master("local[4]").getOrCreate
    import sparkS.implicits._
    import org.apache.spark.sql.functions._

    // Create a DataFrame based on the JSON results.
    val fileName = "/home/nanda/allData/methane.json"
    val df = sparkS.read.json(fileName)  

    val content = df.select(explode($"methane").alias("new_batter"))

    val bat = content.select("new_batter.*")

    val sam = bat.rdd.map(x => (x(2) ,x(0)))

    val sum = Seq(3, 2, 4, 1, 0, 30,30,40,50,-4).toDS
    sum.count.toInt

  }
}