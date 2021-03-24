package mkdir;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @program: Hadoop->mkdir->FirstHdfs
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-13 12:27
 **/
public class FirstHdfs {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        FileSystem fs1 = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://192.168.85.141:9000"), conf);
            fs1 = FileSystem.get(URI.create("file://F://实训3//Hadoop"), conf);
            System.out.println("hdfs:" + "\t" + fs.getClass().getName());
            System.out.println("file:" + "\t" + fs1.getClass().getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}