package tests

/**
 * @author lewis
 */

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

object HDFSscala extends App {
  
  val conf = new Configuration()
  val hdfs = FileSystem.get(URI.create("hdfs://localhost:8020/"), conf)
  val path = new Path("testfile.txt")
  val dos = hdfs.create(path)
  val readBuf = "Hello World2\n".getBytes("UTF-8");
  dos.write(readBuf, 0, readBuf.length);
  dos.write(readBuf, 0, readBuf.length);
  dos.close();

  hdfs.close();
  println("done")
  
}