package tests

/**
 * @author lewis
 */

import functions.Parser

import java.io.BufferedReader;  
import java.io.FileInputStream;  
import java.io.FileReader;  
import java.io.IOException;  
import java.io.InputStreamReader;  

object ParserTest extends App {
  
val br = new BufferedReader(new InputStreamReader(  
                new FileInputStream("./source/pages_fixed_test")));
var i = 0
while(i<100000)  {
  var str = br.readLine()
  if(str!=null) { println(i+":"+Parser.getLog(str))} 
  i += 1
   
  
  
}




        br.close();
  //val str:String = "101.0.33.118 - - [30/Apr/2014:21:31:19 +0000] \"GET /french/splash_inet.html HTTP/1.0\" 200 3781"
  
  //println(str)
  //println(Parser.getLog(str))
  
}