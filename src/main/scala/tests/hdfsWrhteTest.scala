package tests

/**
 * @author lewis
 */

import functions.HDFSWriter

object hdfsWrhteTest extends App {
  val hdfs = new HDFSWriter("hdfs://localhost:8020/", "./web_traffic/testfile")
  hdfs.write("test")
  hdfs.close
  println("done")
}