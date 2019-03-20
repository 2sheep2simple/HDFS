package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author hadoop
 * @see 往HDFS用户目录里写入文件
 */
public class WriteFile {
	public static void main(String[] args) { 
        try {
                Configuration conf = new Configuration();  
                conf.set("fs.defaultFS","hdfs://localhost:9000");
                // 等价写法：conf.set("fs.defaultFS","hdfs://localhost:9000/user/hadoop/");
                conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem fs = FileSystem.get(conf);
                byte[] buff = "Hello world".getBytes(); // 要写入的内容
                String filename = "testanotherfile1"; //要写入的文件名
                FSDataOutputStream os = fs.create(new Path(filename));
                os.write(buff,0,buff.length);
                System.out.println("Create:"+ filename);
                os.close();
                fs.close();
        } catch (Exception e) {  
                e.printStackTrace();  
        }  
}  

}
