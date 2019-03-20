package hdfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author hadoop
 * @see 从HDFS中读取文件
 */
public class ReadFile {
	public static void main(String[] args) {
        try {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS","hdfs://localhost:9000");
                conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem fs = FileSystem.get(conf);
                Path file = new Path("testfile"); 
                FSDataInputStream getIt = fs.open(file);
                BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
                String content = d.readLine(); //读取文件一行
                System.out.println(content);
                d.close(); //关闭文件
                fs.close(); //关闭hdfs文件系统对象，并非执行./sbin/stop-dfs.sh
        } catch (Exception e) {
                e.printStackTrace();
        }
}

}
