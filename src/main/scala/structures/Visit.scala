package structures

/**
 * @author Zhuotao Zhang
 * @date 18/Jun/2015
 */

import java.util.Date
import java.util.UUID
import java.sql.Timestamp

case class Visit(val visitID:String, val visitor:String, val firstHit:Timestamp, var lastHit:Timestamp, var numOfAct:Int, var duraction:Long) extends Serializable {
    
  def this(visitID:String, visitor:String, firstHit:Timestamp, lastHit:Timestamp) = 
    this(visitID, visitor, firstHit, lastHit, 1, 0)
  
  def setLastHit(value:Timestamp) = {
    lastHit = value
    duraction = lastHit.getTime - firstHit.getTime // update the duraction when the lastHit is updated
  }
  
  def addOne = numOfAct += 1
 
  override def toString = "[" + visitID +", " + visitor + ", " + firstHit + ", " + lastHit +"]"
  
}