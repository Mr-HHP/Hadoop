package day5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @program: Hadoop->day5->ListBlock
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-14 14:41
 **/

// hadoop jar HDFS-1.0-SNAPSHOT.jar day5.ListBlock -D input=/test.txt
public class ListBlock extends Configured implements Tool {
    public static void main(String[] args) {
//        Serializable
        try {
            System.exit(ToolRunner.run(
                    new ListBlock(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) {
        // 使用getConf()方法读取Hadoop配置文件
        Configuration conf = getConf();
        // 获取参数
        String input = conf.get("input");
        // 获取HDFS文件系统对象
        FileSystem fs = null;
        // 创建hdfs文件系统输入流
        HdfsDataInputStream hdis = null;
        FSDataInputStream hdis1 = null;
        DFSInputStream dis = null;
        FSDataInputStream fis = null;
        DataInputStream ds = null;
        try {
            fs = FileSystem.get(conf);
            hdis = (HdfsDataInputStream) fs.open(new Path(input));
            ds = (DataInputStream) fs.open(new Path(input));
            hdis1 = fs.open(new Path(input));
            fis = fs.open(new Path(input));

            System.out.println("#####：" + fs.open(new Path(input)));

            dis = (DFSInputStream) hdis.getWrappedStream();
            System.out.println("内嵌的类：" + dis.getClass().getName());
            System.out.println("内嵌的类##：" + hdis1.getWrappedStream().getClass().getName());

            List<LocatedBlock> allBlocks = dis.getAllBlocks();
            List<LocatedBlock> allBlocks1 = hdis.getAllBlocks();
            for (LocatedBlock block : allBlocks) {
                // 获取所有块的各种信息
                System.out.println("块的编号：" + block.getBlock().getBlockId());
                System.out.println("块的名字：" + block.getBlock().getBlockName());
                // 获取块的位置信息
                DatanodeInfo[] datanodeInfos = block.getLocations();
                for (DatanodeInfo datanodeInfo : datanodeInfos) {
                    System.out.println("位置信息：" + datanodeInfo.getName());
                    System.out.println("节点名字：" + datanodeInfo.getHostName() +
                            "\t节点的IP：" + datanodeInfo.getIpAddr());
                }
            }
            System.out.println("===========================================");
            System.out.println("===========================================");
            System.out.println("===========================================");
            byte[] bytes = new byte[1024];
            int length = 0;
            System.out.println("HdfsDataInputStream：");
            while ((length = hdis.read(bytes)) != -1) {
                System.out.println(new String(bytes, 0, length));
            }

            System.out.println("DFSInputStream：");
            length = 0;
            byte[] bytes1 = new byte[1024];
            while ((length = dis.read(bytes1)) != -1) {
                System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                System.out.println(new String(bytes, 0, length));
            }


            System.out.println("DataInputStream：");
            while ((length = dis.read(bytes1)) != -1) {
                System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                System.out.println(new String(bytes, 0, length));
            }


            while ((length = ds.read(bytes)) != -1) {
                System.out.println(new String(bytes, 0, length));
            }



        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关流
            try {
                if (dis != null) dis.close();
                if (hdis1 != null) hdis1.close();
                if (hdis != null) hdis.close();
                if (fis != null) fis.close();
                if (fs != null) fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }
}
