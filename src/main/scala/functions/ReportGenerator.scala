package functions

/**
 * @author Zhuotao ZHang
 * @date 18/Jun/2015
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.sql.Timestamp

/**
 * This object is used for generating metrics from a parquet file
 */
object ReportGenerator {
  
  private var sqlContext:SQLContext = null
  private var filePath:String = null
  private var dataType:String = null // log or visit 
  private var registed = false
  private var df:DataFrame = null
  
  def register(sqlContext: SQLContext, filePath: String, dataType: String) = {
    if(dataType.equals("log")||dataType.equals("visit")) {
      this.sqlContext = sqlContext
      this.filePath = filePath
      df = sqlContext.parquetFile(filePath)
      this.dataType = dataType
      registed = true
    } else {
      throw new Exception("Invalid data type")
    }
  }
  
  /**
   * The functions below are used for 'log' type
   */
  def getNumOfPVs(ts1:Timestamp, ts2:Timestamp):Long = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("log")) throw new Exception("Data tpye doesn't match")
    val res = df.where(df("date") >= ts1 && df("date") < ts2).count()
    return res
  }
  
  def getNumOfUniPVs(ts1:Timestamp, ts2:Timestamp):Long = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("log")) throw new Exception("Data tpye doesn't match")
    val res = df.where(df("date") >= ts1 && df("date") < ts2).groupBy("uri", "visitID").count().count()
    return res
  }
  
  /**
   * The functions below are used for 'visit' type
   */
  def getNumOfVisits(ts1:Timestamp, ts2:Timestamp):Long = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("visit")) throw new Exception("Data tpye doesn't match")
    val res = df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).count()
    return res
  }
  
  def getNumOfUniVisits(ts1:Timestamp, ts2:Timestamp):Long = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("visit")) throw new Exception("Data tpye doesn't match")
    val res = df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).groupBy("visitor").count().count()
    return res
  }
  
  def getNumOfMaxAct(ts1:Timestamp, ts2:Timestamp):Long = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("visit")) throw new Exception("Data tpye doesn't match")
    df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).registerTempTable("max_act")
    val maxAct = sqlContext.sql("SELECT MAX(numOfAct) FROM max_act")
    val res:Long = maxAct.map ( x => x(0) ).take(1).apply(0).toString().toLong
    return res
  }
  
  def getDuraction(ts1:Timestamp, ts2:Timestamp):Double = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("visit")) throw new Exception("Data tpye doesn't match")
    df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).registerTempTable("avg_visit_duraction")
    val avgDuraction = sqlContext.sql("SELECT SUM(duraction) FROM act_per_visit")
    val res:Double = avgDuraction.map ( x => x(0) ).take(1).apply(0).toString().toLong/1000
    return res
  }
  
  def getNumAct(ts1:Timestamp, ts2:Timestamp):Double = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("visit")) throw new Exception("Data tpye doesn't match")
    df.where(df("firstHit") >= ts1 && df("firstHit") < ts2).registerTempTable("act_per_visit")
    val maxAct = sqlContext.sql("SELECT SUM(numOfAct) FROM act_per_visit")
    val res:Double = 100*maxAct.map ( x => x(0) ).take(1).apply(0).toString().toLong
    return res
  }
  
  def getBounceCount(ts1:Timestamp, ts2:Timestamp):Double = {
    if(!registed) throw new Exception("Please regist first")
    if(!dataType.equals("visit")) throw new Exception("Data tpye doesn't match")
    val res:Double = df.where(df("firstHit") >= ts1 && df("firstHit") < ts2 && df("numOfAct") > 1).count()
    return res
  }
  
}