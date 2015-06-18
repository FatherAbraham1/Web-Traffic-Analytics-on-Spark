package functions

/**
 * @author Zhuotao Zhang
 * @date 27/May/2015
 */

import java.text.SimpleDateFormat
import java.util.regex._
import java.util.Locale
import java.util.Date
import structures.Log

/**
 * Parser is a singleton which can parse the input log string into a Log class
 * The simple log format is shown as follows.
 * 
 * 101.0.33.118 - - [30/Apr/2014:21:31:19 +0000] 'GET /french/splash_inet.html HTTP/1.0' 200 3781
 *  
 * The log format above contain five parts, which are user ID, date, action, result and the size of bytes
 * User can modify parts of the regex pattern settings to make Parer to suitable for you log file
 */

object Parser {
  
  /**
  * The string 'userIdPattern' is the pattern for user ID.
  * The default setting asume that the log file is using IP to indicate users.
  */
  private var userIdPattern = "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}"
  
  private var datePattern = "[^]]+" //"(?<=\\[).*(?=\\])"//.r
  
  /**
   * An action contain three different parts, http request type, URI and http protocol version
   */
  private var actionPattern = "[^']+"
  
  private var resultPattern = "[0-9]{3}"
  
  private var dataSizePattern = "[0-9]*"
  
  /**
   * The string 'dateFormat' is used for defining the format to generate a Date for Log
   */
  private var dateFormat = "dd/MMM/yyyy:H:mm:ss Z"
  
  private var regex = "("+userIdPattern+")\\s-\\s-\\s\\[("+datePattern+")\\]\\s\\\"("+actionPattern+")\\\"\\s("+resultPattern+")\\s("+dataSizePattern+")"
  
  private var pattern = Pattern.compile(regex)
  
  
  
  /**
   * @param str : One line in log file
   * @return  An instance of Log
   * @see structures.Log
   */
  def getLog(str:String):Log = {
    //println(str)
    val matcher = pattern.matcher(str);
    
    if (matcher.find()) {    
      
      val userID = matcher.group(1)
      //Set dateFormat
      val sdf = new SimpleDateFormat(dateFormat,Locale.ENGLISH)
      val date =  sdf.parse(matcher.group(2)).getTime
      
      //Split the action to three fields
      val act = matcher.group(3).split(" ")
      val actType = act.apply(0)
      val uri = act.apply(1)
      
      val httpVersion = act.apply(2)
      val result = matcher.group(4)
      
      /**
       * This field in some logs may be "-", set it to zero
       */
      val dataSize = try {
        matcher.group(5).toLong
      } catch {
        case e:NumberFormatException => 0
      }
      
      return Log(userID, date, actType, uri, httpVersion, result, dataSize)
      
    } else { return null }
    
  }
  
  def setUserIdPattern(str:String) = { 
    userIdPattern = str
    regex = "("+userIdPattern+")\\s-\\s-\\s\\[("+datePattern+")\\]\\s\\\"("+actionPattern+")\\\"\\s("+resultPattern+")\\s("+dataSizePattern+")"
  }
  
  def setDatePattern(str:String) = {
    datePattern = str
    regex = "("+userIdPattern+")\\s-\\s-\\s\\[("+datePattern+")\\]\\s\\\"("+actionPattern+")\\\"\\s("+resultPattern+")\\s("+dataSizePattern+")"
  }
  
  def setActionPattern(str:String) = {
    actionPattern = str
    regex = "("+userIdPattern+")\\s-\\s-\\s\\[("+datePattern+")\\]\\s\\\"("+actionPattern+")\\\"\\s("+resultPattern+")\\s("+dataSizePattern+")"
  }
  
  def setResultPattern(str:String) = {
    resultPattern = str
    regex = "("+userIdPattern+")\\s-\\s-\\s\\[("+datePattern+")\\]\\s\\\"("+actionPattern+")\\\"\\s("+resultPattern+")\\s("+dataSizePattern+")"
  }
  
  def setDataSizePattern(str:String) = {
    dataSizePattern = str
    regex = "("+userIdPattern+")\\s-\\s-\\s\\[("+datePattern+")\\]\\s\\\"("+actionPattern+")\\\"\\s("+resultPattern+")\\s("+dataSizePattern+")"
  }
  
  /**
   * This is designed for those log formats which are totally different from the normal format.
   * User can define the regex directly by calling this mathod.
   * To be noticed, it should at least contain five fields which we have mentioned above.
   */
  def setRegex(str:String) = {
    regex = str
  }
  
  /**
   * Set JAVA SimpleDateFormat
   */
  def setDateFormat(str:String) = {
    dateFormat = str
  }
  
}