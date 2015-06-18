package tests

/**
 * @author lewis
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;

import com.tresata.spark.sorted.PairRDDFunctions._

import structures._
import functions._

import java.util.Date
import java.util.UUID
import java.io._
import java.sql.Timestamp

object SparkSortedTest extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._
  
  
  val hw = new HDFSWriter("hdfs://localhost:8020/", "./web_traffic/log_file_with_visitID")
  
  def getVisit(l:List[Visit], v:Log):List[Visit] = {
    if(l.isEmpty) {
      val visitID = v.userID+"@"+v.date
      hw.write(v.toString()+","+visitID+"\n")
      return l.:+(new Visit(visitID, v.userID, new Timestamp(v.date), new Timestamp(v.date)))
    } else {
      if(v.date-l.last.lastHit.getTime > 30*60*1000) {
        val visitID = v.userID+"@"+v.date
        hw.write(v.toString()+","+visitID+"\n")
        return l.:+(new Visit(visitID, v.userID, new Timestamp(v.date), new Timestamp(v.date)))
      } else {
        val visitID = v.userID+"@"+l.last.firstHit.getTime
        hw.write(v.toString()+","+visitID+"\n")
        l.last.setLastHit(new Timestamp(v.date))
        l.last.addOne
        return l
      }
    }
  }
  
  val pairs:RDD[Tuple2[String,Log]] = sc.textFile("hdfs://localhost:8020/user/lewis/log_data/pages_fixed_test")
    .map { s:String => Tuple2(s.split(" ").apply(0), Parser.getLog(s))}
  
  //pairs.foreach(println)
  
  val visits:RDD[Visit] = pairs
    .groupSort(Ordering.by[Log, Long](_.date))//groupsorted by key
    .foldLeftByKey(List():List[Visit]){(l,v) => getVisit(l,v)}//return (ip,list of visits of this ip)
    .flatMap{x => x._2}//get visits rdd
    
  //visits.saveAsTextFile("hdfs://localhost:8020/user/lewis/web_traffic/visit_file")
  
  visits.toDF().saveAsParquetFile("hdfs://localhost:8020/user/lewis/web_traffic/visit_test.parquet")
  hw.close
    
  //val df = visits.toDF().registerTempTable("visit")
  
  // val df = visits.toDF().saveAsTable("visit_test")
  
  
  
  //sqlContext.sql("SELECT count(*) FROM visit").foreach(println)
  
  def getNewLog(str:String):LogWithVisitID = {
    val arr = str.split(",")
    return LogWithVisitID(arr.apply(0), new Timestamp(arr.apply(1).toLong),arr.apply(2),arr.apply(3),arr.apply(4),arr.apply(5),arr.apply(6).toLong,arr.apply(7))
  }
  
  val logsWithVisitID:RDD[LogWithVisitID] = sc.textFile("hdfs://localhost:8020/user/lewis/web_traffic/log_file_with_visitID")
    .map { x => getNewLog(x) }//generate LogWithVisitID
  
  val df = logsWithVisitID.toDF().registerTempTable("log")//userID
  
  println(sqlContext.sql("SELECT distinct(userID) FROM log").count())
  
  println(logsWithVisitID.count())
  
  val visit2 = sqlContext.parquetFile("hdfs://localhost:8020/user/lewis/web_traffic/visit_test.parquet").registerTempTable("visit_parquet")
  val res2 = sqlContext.sql("SELECT count(*) FROM visit_parquet")
  
  res2.foreach(println)
  //println(visit2.count())
  
  
  
  //logsWithVisitID.foreach(println)
  
  //logsWithVisitID.toDF().saveAsParquetFile("log_test_final4.parquet")
  
  //logsWithVisitID.foreach(println)
  
  //writer.close()

 

  

}