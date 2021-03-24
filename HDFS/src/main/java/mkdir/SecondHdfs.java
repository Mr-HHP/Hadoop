package mkdir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @program: Hadoop->mkdir->SecondHdfs
 * @description: 把/etc/passwd文件写到hdfs文件系统中的hdfs用户下
 * @author: Mr.黄
 * @create: 2019-10-13 14:42
 **/

/**
 * Configured提供了getConf(),通过该方法可以获取配置文件
 *     /opt/hadoop/etc/hadoop下的core-site.xml hdfs-site.xml
 *       yarn-site.xml mapred-site.xml等
 *
 *     给main函数传参数
 *     hadoop jar jar包  类的全限定名 参数1 参数2
 *     hadoop jar Hdfs-1.0-SNAPSHOT.jar com.briup.FirstHdfs
 *         hdfs://192.168.43.127:9000
 *         hdfs://192.168.43.127:9000/user/hdfs/hdfstest.txt
 *         -D input=test.txt
 *
 *    Tool接口是工具类，帮助我们执行程序
 *     hadoop jar jar包  类的全限定名  参数 -D 变量名=值  -D 变量名=值
 *     -D 变量名=值  -D 变量名=值
 *     注意：tool工具可以帮助把-D后面的变量设置到configuration中
 *     取
 *     conf.get(变量名)
 *
 *     hadoop jar Hdfs-1.0-SNAPSHOT.jar mkdir.SecondHdfs
 *     -D input=/etc/passwd -D output=/second.txt
 *
 *     目的：把/etc/passwd 写入到hdfs集群中的hdfs用户下
 */
public class SecondHdfs extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new SecondHdfs(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) {
        // 读取Hadoop配置文件，获取hdfs入口，getConf()方法会自动读取Hadoop集群的配置文件
        Configuration conf = getConf();
        // -D input=/etc/passwd -D output=/second.txt
        // 文件读取路径
        String input = conf.get("input");
        // 文件写入路径
        String output = conf.get("output");
        // 获取hdfs文件系统
        FileSystem fs = null;
        // /etc/passwd文件读取流
        BufferedInputStream bis = null;
        // 文件写入流
        BufferedOutputStream bos = null;
        try {
            fs = FileSystem.get(conf);
            System.out.println(fs.getClass().getName());
            bis = new BufferedInputStream(
                    new FileInputStream(input));
//            bos = new BufferedOutputStream(fs.create(new Path(output), (short) 10));
            bos = new BufferedOutputStream(
                    fs.create(new Path(output)));
            // 写入
            byte[] bytes = new byte[1024];
            int length = 0;
            while ((length = bis.read(bytes)) != -1) {
                bos.write(bytes, 0, length);
                bos.flush();
            }
            bos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bos != null) {
                    bos.close();
                }
                if (bis != null) {
                    bis.close();
                }
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }
}
