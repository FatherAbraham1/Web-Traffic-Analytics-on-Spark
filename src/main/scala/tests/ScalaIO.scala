package tests

import java.io._

/**
 * @author lewis
 */
object ScalaIO extends App {
  val writer = new PrintWriter(new File("scala_io_test" ))
      writer.write("Hello Scala\n")
      writer.write("Hello Scala\n")
      writer.close()
}