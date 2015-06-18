package tests

/**
 * @author lewis
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;

object parquetTest extends App {
  
  val conf = new SparkConf().setAppName("Simple Application 2").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
  val parquetFile = sqlContext.parquetFile("visit_test.parquet")

  //Parquet files can also be registered as tables and then used in SQL statements.
  parquetFile.registerTempTable("parquetFile")
  val res = sqlContext.sql("SELECT * FROM parquetFile WHERE numOfAct = 836")
  
  res.foreach(println)
  
}