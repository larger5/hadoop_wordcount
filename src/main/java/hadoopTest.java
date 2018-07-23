import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class hadoopTest {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
//这里指定使用的是 hdfs 文件系统
     conf.set("fs.defaultFS", "hdfs://localhost:9000");
//通过如下的方式进行客户端身份的设置
      System.setProperty("HADOOP_USER_NAME", "root");
//通过 FileSystem 的静态方法获取文件系统客户端对象
        FileSystem fs = FileSystem.get(conf);
//也可以通过如下的方式去指定文件系统的类型 并且同时设置用户身份
//FileSystem fs = FileSystem.get(new URI("hdfs://node-21:9000"), conf, "root");
//创建一个目录
        fs.create(new Path("/test"), false);
//上传一个文件
  //      fs.copyFromLocalFile(new Path("e:/hello.sh"), new Path("/hdfsbyjava-ha"));
//关闭我们的文件系统
        fs.close();

    }

}
