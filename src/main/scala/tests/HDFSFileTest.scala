package tests

/**
 * @author lewis
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import structures._
import functions._
import java.sql.Timestamp


object HDFSFileTest extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  
  
  def getNewLog(str:String):LogWithVisitID = {
    val arr = str.split(",")
    return LogWithVisitID(arr.apply(0), new Timestamp(arr.apply(1).toLong),arr.apply(2),arr.apply(3),arr.apply(4),arr.apply(5),arr.apply(6).toLong,arr.apply(7))
  }
  
  val logsWithVisitID:RDD[LogWithVisitID] = sc.textFile("hdfs://localhost:8020/user/lewis/web_traffic/log_file_with_visitID")
    .map { x => getNewLog(x) }//generate LogWithVisitID
  
  logsWithVisitID.foreach(println)
}