package tests;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFStest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//FileSystem hdfs = FileSystem.get(new Configuration());
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:8020/"), conf);
		Path path = new Path("testfile.txt");

		// writing
		FSDataOutputStream dos = hdfs.create(path);
		byte[] readBuf = "Hello World\n".getBytes("UTF-8");
		dos.write(readBuf, 0, readBuf.length);
		dos.write(readBuf, 0, readBuf.length);
		dos.close();

		hdfs.close();
		System.out.println("done");
	}

}
