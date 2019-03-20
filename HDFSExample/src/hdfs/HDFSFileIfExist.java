package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileIfExist {
	public static void main(String[] args){
        try{
            String fileName = "input";  
            // 相对了路径的表示方法,即表示/usr/hadoop/input
            // 若定义成 String fileName = “/input”  则表示 HDFS根目录下的 /input,此目录和 /usr 目录平行
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(fileName))){
                System.out.println("文件存在");
            }else{
                System.out.println("文件不存在");
            }
 
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
