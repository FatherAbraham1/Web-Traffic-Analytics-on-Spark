package structures

/**
 * @author Zhuotao Zhang
 * @date 24/May/2015
 */

import java.sql.Timestamp

case class LogWithVisitID(userID:String, date:Timestamp, actType:String, uri:String, httpVersion:String, result:String, dataSize:Long, visitID:String) extends Serializable {
  
  override def toString():String = 
    userID + "," + date + "," + actType + "," + uri + "," + httpVersion + "," + result + "," + dataSize + "," + visitID
  
}
