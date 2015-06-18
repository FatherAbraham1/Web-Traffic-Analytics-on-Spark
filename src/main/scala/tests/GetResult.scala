package tests

/**
 * @author lewis
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GetResult extends App {
  
  val conf = new SparkConf().setAppName("Simple Application 2").setMaster("local[2]")//.set( "spark.executor.memory" , "3g" )
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
  val parquetFileVisit = sqlContext.parquetFile("hdfs://localhost:8020/"+"/user/lewis/web_traffic/idsite_"+2+"/visits.parquet")

  //Parquet files can also be registered as tables and then used in SQL statements.
  parquetFileVisit.registerTempTable("visits")
  
  val maxAct = sqlContext.sql("SELECT MAX(numOfAct) FROM visits")
  val resMaxAct:Long = maxAct.map ( x => x(0) ).take(1).apply(0).toString().toLong
  

  val numberOfVisit = sqlContext.sql("SELECT * FROM visits")
  val resNumberOfVisit:Long = numberOfVisit.count()
  
  
  val uniqVisit = sqlContext.sql("SELECT visitor FROM visits GROUP BY visitor")
  val resUniqVisit:Long = uniqVisit.count()
  
  val parquetFileLog = sqlContext.parquetFile("hdfs://localhost:8020/"+"/user/lewis/web_traffic/idsite_"+2+"/logs.parquet")
  parquetFileLog.registerTempTable("logs")
  val res2 = sqlContext.sql("SELECT userID FROM logs GROUP BY userID").count()
  
  println("Maximum action in one visit: "+resMaxAct)
  println("Visits: "+resNumberOfVisit)
  println("Unique Visits: "+resUniqVisit)
  
  val test = sqlContext.sql("SELECT * FROM visits WHERE numOfAct = 836").show()

  println(test)
  
}