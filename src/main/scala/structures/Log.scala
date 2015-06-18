package structures

/**
 * @author Zhuotao Zhang
 * @date 24/May/2015
 */

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date
import java.sql.Timestamp

case class Log(userID:String, date:Long, actType:String, uri:String, httpVersion:String, result:String, dataSize:Long) extends Serializable {
  
  override def toString():String = 
    userID + "," + date + "," + actType + "," + uri + "," + httpVersion + "," + result + "," + dataSize
    
}