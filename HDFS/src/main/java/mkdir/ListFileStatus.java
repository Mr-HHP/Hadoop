package mkdir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Arrays;

/**
 * @program: Hadoop->mkdir->ListFileStatus
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-14 00:44
 **/

/*
hadoop jar HDFS-1.0-SNAPSHOT.jar mkdir.ListFileStatus -D input=/second.txt
 */
public class ListFileStatus extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.out.println(Arrays.toString(args));
            System.exit(ToolRunner.run(new ListFileStatus(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // getConf()会自动读取Hadoop配置文件
        Configuration conf = getConf();
        // 获取参数
        String input = conf.get("input");
        // 获取hdfs文件系统
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(input));
        System.out.println("==============" + fileStatuses.length);
//        FileStatus[] fileStatuses1 = fs.listStatus(new File(input));
        for (FileStatus f : fileStatuses) {
            System.out.println("备份：" + f.getReplication());
            System.out.println("块大小：" + f.getBlockSize());
            System.out.println("权限：" + f.getPermission());
            System.out.println("所属组：" + f.getGroup());
            System.out.println("拥有者：" + f.getOwner());
            System.out.println("路径：" + f.getPath());
        }
        return 0;
    }
}
