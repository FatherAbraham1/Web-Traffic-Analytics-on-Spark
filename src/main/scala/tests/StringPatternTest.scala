package tests

/**
 * @author lewis
 */

import java.util.regex._



object StringPatternTest extends App {
  
  
  val str:String = "101.0.33.118 - - [30/Apr/2014:21:31:19 +0000] 'GET /french/splash_inet.html HTTP/1.0' 200 3781"
  
  val userID = "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}"
  
  val date = "[^]]+" //"(?<=\\[).*(?=\\])"//.r
  
  val action = "[^']+"
  
  val result = "[0-9]{3}"
  
  val byte = "[0-9]*"
  
  val pattern = Pattern.compile("("+userID+")\\s-\\s-\\s\\[("+date+")\\]\\s\\'("+action+")\\'\\s("+result+")\\s("+byte+")");
  
  val matcher = pattern.matcher(str);
  
  if (matcher.find()) {
    println(matcher.group(1));
    println(matcher.group(2));
    println(matcher.group(3));
    println(matcher.group(4));
    println(matcher.group(5));
  }

  println("test")
  
  //println(date.findFirstIn(str))
  //println(str.split(" ").apply(6))
  
  
  

}