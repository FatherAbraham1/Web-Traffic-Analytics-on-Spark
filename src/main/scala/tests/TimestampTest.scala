package tests

/**
 * @author lewis
 */

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date

object TimestampTest extends App {
  val dateTimeFormat = new SimpleDateFormat("dd/MMM/yyyy:H:mm:ss Z",Locale.ENGLISH)
  val day = "25/Jun/1990:00:00:00 +0000" 
  val dateTime:Date = dateTimeFormat.parse(day)
  println(dateTime)
  val timestamp = new Timestamp(dateTime.getTime)
  println(timestamp)
}