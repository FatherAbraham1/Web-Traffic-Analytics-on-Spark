package functions

/**
 * @author Zhuotao Zhang
 * @date 8/Jul/2015
 */

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD;
import com.tresata.spark.sorted.PairRDDFunctions._
import structures._
import functions._
import java.util.Date
import java.sql.Timestamp

object VisitCalculator {
  
  //private val GENERATE_ALIVE_VISITS = true
  
  private var conf:SparkConf = null
  private var hw:HDFSWriter = null //new HDFSWriter("hdfs://localhost:8020", "./web_traffic/log.tmp") 
  private var filePath:String = null
  private var usrPath:String = null
  private var hdfs:String = null
  private var idsite:Int = _
  private var registed = false
  private val thirtyMins = 30*60*1000
  
  def register(conf: SparkConf, hdfs:String, filePath:String, usrPath:String, idsite:Int) {
    this.conf = conf
    this.hw =  new HDFSWriter(hdfs, "./web_traffic/log.tmp") 
    this.filePath = filePath
    this.hdfs = hdfs
    this.usrPath = usrPath
    this.idsite = idsite
    this.registed = true  
  }
  
  def calculate() {
    
    if(!registed) throw new Exception("Please regist first")
    
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
    
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    
    
    val rdd:RDD[String] = sc.textFile(filePath)
    
    val pairs:RDD[Tuple2[String,Log]] = rdd.map { s:String => Tuple2(s.split(" ").apply(0), Parser.getLog(s))}
 
    val visits:RDD[Visit] = pairs
    .groupSort(Ordering.by[Log, Long](_.date))//groupsorted by key
    .foldLeftByKey(List():List[Visit]){(l,v) => getVisit(l,v)}//return (ip,list of visits of this ip)
    .flatMap{x => x._2}//get visits rdd
    
    visits.toDF().saveAsParquetFile(hdfs+usrPath+"/web_traffic/idsite_"+idsite+"/visits.parquet")
    
    /*
     * A expired logic for "alive" visits
     * There are 3 different cases of definition of ending time.
     * 1)Ending time of the log file
     * 2)The current time
     * 3)Endding time of the day
     * It depends on how user use this class for batch job
     */
    val endingTime = Timestamp.valueOf("2014-05-01 17:29:04")
    //val endingTime = new Timestamp(System.currentTimeMillis())
    //val endingTime = Timestamp.valueOf("2014-05-01 00:00:00")
    
    val hw2 = new HDFSWriter(hdfs, "./web_traffic/idsite_"+idsite+"/alive_visit.tmp")
    
    val df = sqlContext.parquetFile(hdfs+usrPath+"/web_traffic/idsite_"+idsite+"/visits.parquet")
    //val df = visits.toDF()
    df.where(df("lastHit") >= new Timestamp(endingTime.getTime - thirtyMins) && df("alive")).collect()
      .foreach { x => hw2.write(x.toString()+"\n") }
    
      //.saveAsParquetFile(hdfs+usrPath+"/web_traffic/idsite_"+idsite+"/alive_visits.parquet")
    
    hw.close
    
    val logsWithVisitID:RDD[LogWithVisitID] = sc.textFile(hdfs+usrPath+"/web_traffic/log.tmp")
      .map { x => getNewLog(x) }//generate LogWithVisitID
  
    logsWithVisitID.toDF().saveAsParquetFile(hdfs+usrPath+"/web_traffic/idsite_"+idsite+"/logs.parquet")
    
  }
  
  private def getVisit(l:List[Visit], v:Log):List[Visit] = {
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
  
  private def getNewLog(str:String):LogWithVisitID = {
    val arr = str.split(",")
    return LogWithVisitID(arr.apply(0), new Timestamp(arr.apply(1).toLong),arr.apply(2),arr.apply(3),arr.apply(4),arr.apply(5),arr.apply(6).toLong,arr.apply(7))
  }
  
}