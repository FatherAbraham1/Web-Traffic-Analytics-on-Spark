package functions

/**
 * @author Zhuotao Zhang
 * @date 24/May/2015
 */

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class HDFSWriter(hdfsURI:String, filePath:String) extends Serializable {
  
  private val hdfs = FileSystem.get(URI.create(hdfsURI), new Configuration())
  private val path = new Path(filePath)
  private val out = hdfs.create(path)
  
  def write(str:String) = {
    val readBuf = str.getBytes("UTF-8");
    out.write(readBuf, 0, readBuf.length)
  }  
  
  def close = out.close()
  
}