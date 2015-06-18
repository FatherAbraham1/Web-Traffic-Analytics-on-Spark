package tests

/**
 * @author lewis
 */

import functions.ToMySQL
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat

object MySQLTest extends App {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    
    val dateFormat = "dd/MMM/yyyy"
    val sdf = new SimpleDateFormat(dateFormat)
    val date1 =  new java.sql.Date(sdf.parse("25/Jan/2014").getTime)
    val date2 =  new java.sql.Date(sdf.parse("25/Jan/2014").getTime)
    
    
    var ts = new Timestamp(System.currentTimeMillis());  
    val tsStr = "2014-05-09 11:49:45"
    try {  
        ts = Timestamp.valueOf(tsStr);  
    } catch {  
        case e: Exception => println(e)
    }  
    
    val value: Double = 100
     
    val data = sc.parallelize(List((100, "test5", 2, date1, date2 ,1, ts, value)))
    ToMySQL.setConn(DriverManager.getConnection("jdbc:mysql://localhost:3306/log_data", "root", "root"))
    ToMySQL.setTableName("piwik_archive_numeric_2014_01")
    data.foreachPartition(ToMySQL.insert)
}