package functions

/**
 * @author Zhuotao Zhang
 * @date 17/Jun/2015
 */

import java.sql.{DriverManager, PreparedStatement, Connection, Date, Timestamp}

/**
 * This object is used to store the report data to MySQL 
 * Variables 'conn' and 'tableName' should be set before executing 'insert' function
 */

object ToMySQL {
  
  private var conn: Connection = null  
  private var tableName: String = null
  
  //case class Archive(idarchive: Int, name: String, idsite: Int, date1: Date, date2: Date, period: Int, ts_archived: Timestamp, value: Double)
  
  def insert(iterator: Iterator[(Int, String, Int, Date, Date, Int, Timestamp, Double)]): Unit = {
    
    if(conn!=null&&tableName!=null) {
      val sql = "insert into "+tableName+"(idarchive, name, idsite, date1, date2, period, ts_archived, value) values (?, ?, ?, ?, ?, ?, ?, ?)"
      var ps: PreparedStatement = null
      try {
        Class.forName("com.mysql.jdbc.Driver")
        iterator.foreach(
          data => {
          ps = conn.prepareStatement(sql)
          ps.setInt(1, data._1)
          ps.setString(2, data._2)
          ps.setInt(3, data._3)
          ps.setDate(4, data._4)
          ps.setDate(5, data._5)
          ps.setInt(6, data._6)
          ps.setTimestamp(7, data._7)
          ps.setDouble(8, data._8)
          ps.executeUpdate()
          }
        )
      } catch {
        case e: Exception => println(e)
      } 
    } else {
      throw new Exception("Please set connection and table name.")
    }
  }
  
  def setTableName(str:String) = tableName = str
  
  def setConn(c: Connection) = conn = c

}