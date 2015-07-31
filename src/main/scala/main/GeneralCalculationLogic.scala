package main

/**
 * @author lewis
 * @date 14/Jul/2015
 */

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
import structures._
import functions._
import com.tresata.spark.sorted.PairRDDFunctions._
import java.sql.{DriverManager, Timestamp}

object GeneralCalculationLogic extends App {
  
  /**
   * Getting Visit
   */
  val thirtyMins = 30*60*1000
  val hw =  new HDFSWriter("hdfs://localhost:8020", "./web_traffic/log.tmp")
  val idsite = 2
  
  def getVisit(l:List[Visit], v:Log):List[Visit] = {
  if(l.isEmpty) {
    val visitID = v.userID+"@"+v.date
    hw.write(v.toString()+","+visitID+"\n")
    return l.:+(new Visit(visitID, v.userID, new Timestamp(v.date), new Timestamp(v.date)))
  } else {
      if(v.date-l.last.lastHit.getTime > thirtyMins) {
        /*
         * The visit last for more than 30 mins without new events until the end 
         * Those visit should dead but can not be recognized as dead
         * Which means another expire logic is required after generating visits
         */
        l.last.kill 
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
  
  def getNewLog(str:String):LogWithVisitID = {
    val arr = str.split(",")
    return LogWithVisitID(arr.apply(0), new Timestamp(arr.apply(1).toLong),arr.apply(2),arr.apply(3),arr.apply(4),arr.apply(5),arr.apply(6).toLong,arr.apply(7))
  }
  
  var conf = new SparkConf().setAppName("CalculationLogic").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
  
  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._
  
  val rdd:RDD[String] = sc.textFile("hdfs://localhost:8020/user/lewis/log_data/pages_fixed_test")  
  val pairs:RDD[Tuple2[String,Log]] = rdd.map { s:String => Tuple2(s.split(" ").apply(0), Parser.getLog(s))}
 
  /*
   * Visit
   */
  val visits:RDD[Visit] = pairs
  .groupSort(Ordering.by[Log, Long](_.date))//groupsorted by key
  .foldLeftByKey(List():List[Visit]){(l,v) => getVisit(l,v)}//return (ip,list of visits of this ip)
  .flatMap{x => x._2}.cache()//get visits rdd
  
  val df = visits.toDF()// visit dataframe
  
  /** 
   * Generating Report
   */
  
  var ts1:Timestamp = null
  var ts2:Timestamp = null  
  try {  
      ts1 = Timestamp.valueOf("2014-04-30 01:00:00");  
      ts2 = Timestamp.valueOf("2014-05-02 01:00:00");  
  } catch {  
      case e: Exception => println(e)
  }  
  
  val nbVisits:Double = df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).count()
  val nbUniqvisitors:Double = df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).groupBy("visitor").count().count()
  
  df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).registerTempTable("max_act")
  val maxAct = sqlContext.sql("SELECT MAX(numOfAct) FROM max_act")
  val maxActions:Double = maxAct.map ( x => x(0) ).take(1).apply(0).toString().toDouble
  
  df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).registerTempTable("act_per_visit")
  val numAct = sqlContext.sql("SELECT SUM(numOfAct) FROM act_per_visit")
  //val nbActions:Double = 100*numAct.map ( x => x(0) ).take(1).apply(0).toString().toDouble
  
  val bounceCount:Double = df.where(df("firstHit") >= ts1 && df("firstHit") < ts2 && df("numOfAct") > 1).count()
  
  df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).registerTempTable("avg_visit_duraction")
  val avgDuraction = sqlContext.sql("SELECT SUM(duraction) FROM act_per_visit")
  val sumVisitLength:Double = avgDuraction.map ( x => x(0) ).take(1).apply(0).toString().toLong/1000
  
  hw.close
  
  /*
   * Log
   */
  val logsWithVisitID:RDD[LogWithVisitID] = sc.textFile("hdfs://localhost:8020/user/lewis/web_traffic/log.tmp")
    .map { x => getNewLog(x) }//generate LogWithVisitID
  
  val df2 = logsWithVisitID.toDF()// log dataframe
  val actionsNbPageviews:Double = df2.where(df2("date") >= ts1 && df2("date") < ts2).count()
  val actionsNbUniqpageviews:Double = df2.where(df2("date") >= ts1 && df2("date") < ts2).groupBy("uri", "visitID").count().count()
  
  /**
   * Store the report to MySQL
   */
  val reportId = 16
  //case class Archive(idarchive: Int, name: String, idsite: Int, date1: Date, date2: Date, period: Int, ts_archived: Timestamp, value: Double)
  val data = sc.parallelize(List(
    /*The second parameter is set to match the table in Piwik*/
    (reportId, "Actions_nb_pageviews", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), actionsNbPageviews),
    (reportId, "Actions_nb_uniq_pageviews", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), actionsNbUniqpageviews),
    (reportId, "nb_visits", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), nbVisits),
    (reportId, "nb_uniq_visitors", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), nbUniqvisitors),
    (reportId, "max_actions", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), maxActions),
    (reportId, "nb_actions", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), actionsNbPageviews),
    (reportId, "bounce_count", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), bounceCount),
    (reportId, "sum_visit_length", idsite, new java.sql.Date(ts1.getTime), new java.sql.Date(ts2.getTime) ,1, new Timestamp(System.currentTimeMillis()), sumVisitLength)
  ))
  
  val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/log_data", "root", "root")
  ToMySQL.setConn(conn)
  ToMySQL.setTableName("piwik_archive_numeric_2014_01")
  data.foreachPartition(ToMySQL.insert)
  conn.close
  
  println("Done!")
}

