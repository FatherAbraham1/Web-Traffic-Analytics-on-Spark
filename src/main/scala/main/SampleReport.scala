package main

/**
 * @author Zhuotao Zhang
 * @date 18/Jun/2015
 */

import functions.{ToMySQL, ReportGenerator}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import java.sql.{DriverManager, Timestamp}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * This is sample app for generating report and store it to a piwik archive numeric table.
 * @see functions.ToMySQL.scala
 * @see functions.ReportGenerator.scala
 */

object SampleReport extends App {
  
  val conf = new SparkConf().setAppName("Report").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val idsite = 2
  
  /*
   * Get report results
   */
  
  ReportGenerator.register(
    sqlContext, 
    "hdfs://localhost:8020/"+"/user/lewis/web_traffic/idsite_"+idsite+"/logs.parquet", 
    "log"
  )  
  var ts1:Timestamp = null
  var ts2:Timestamp = null  
  try {  
      ts1 = Timestamp.valueOf("2014-04-30 01:00:00");  
      ts2 = Timestamp.valueOf("2014-05-01 01:00:00");  
  } catch {  
      case e: Exception => println(e)
  }  
  val actionsNbPageviews:Double = ReportGenerator.getNumOfPVs(ts1,ts2)
  val actionsNbUniqpageviews:Double = ReportGenerator.getNumOfUniPVs(ts1, ts2)
  
  ReportGenerator.register(
    sqlContext, 
    "hdfs://localhost:8020/"+"/user/lewis/web_traffic/idsite_"+idsite+"/visits.parquet", 
    "visit"
  )  
  val nbVisits:Double = ReportGenerator.getNumOfVisits(ts1, ts2)
  val nbUniqvisitors:Double = ReportGenerator.getNumOfUniVisits(ts1, ts2)
  val maxActions:Double = ReportGenerator.getNumOfMaxAct(ts1, ts2)
  val nbActions:Double = ReportGenerator.getNumAct(ts1, ts2)// The same as actionsNbPageviews
  val bounceCount:Double = ReportGenerator.getBounceCount(ts1, ts2)
  val sumVisitLength:Double = ReportGenerator.getDuraction(ts1, ts2)
  
  /*
   * Store the report to MySQL
   */
  
  //case class Archive(idarchive: Int, name: String, idsite: Int, date1: Date, date2: Date, period: Int, ts_archived: Timestamp, value: Double)
  val data = sc.parallelize(List(
    /*The second parameter is set to match the table in Piwik*/
    (5, "Actions_nb_pageviews", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), actionsNbPageviews),
    (5, "Actions_nb_uniq_pageviews", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), actionsNbUniqpageviews),
    (5, "nb_visits", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), nbVisits),
    (5, "nb_uniq_visitors", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), nbUniqvisitors),
    (5, "max_actions", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), maxActions),
    (5, "nb_actions", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), nbActions),
    (5, "bounce_count", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), bounceCount),
    (5, "sum_visit_length", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), sumVisitLength)
  ))
  
  val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/log_data", "root", "root")
  ToMySQL.setConn(conn)
  ToMySQL.setTableName("piwik_archive_numeric_2014_01")
  data.foreachPartition(ToMySQL.insert)
  conn.close
  
}