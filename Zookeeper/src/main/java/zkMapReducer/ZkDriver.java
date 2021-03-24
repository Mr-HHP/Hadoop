package zkMapReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: Hadoop->PACKAGE_NAME->zkMapReducer.ZkDriver
 * @description: 使用zookeeper管理MapReducer，使多个Reducer能顺序向数据库插入数据
 * @author: Mr.黄
 * @create: 2019-11-02 19:34
 **/
public class ZkDriver extends Configured implements Runnable {
//    private Configuration conf = getConf();
    private Configuration conf;

    public ZkDriver() {
    }

    public ZkDriver(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void run() {
        //



        // 使用getConf()读取配置文件
//        Configuration conf = getConf();
        System.out.println("ZkDriver的conf：\t" + conf);
        // 获取参数
//        String input = conf.get("input");
//        String output = conf.get("output");

        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");

        // 创建作业
        Job job = null;
        try {
            job = Job.getInstance(conf);

            // 设置主类入口
            job.setJarByClass(ZkDriver.class);
            job.setJobName("hhp_zookeeper");

            // 设置数据库连接要素
            DBConfiguration.configureDB(job.getConfiguration(),
                    "com.mysql.jdbc.Driver",
                    "jdbc:mysql://192.168.85.143:3306/briup",
                    "root", "root");

            // 设置SQL语句（insert语句）
            DBOutputFormat.setOutput(job,
                    "teacher",
                    "name", "age");

            // 设置Mapper组件
            job.setMapperClass(ZkMapper.class);
            // 设置Mapper输出的key类型
            job.setMapOutputKeyClass(Teacher.class);
            // 设置Mapper输出的value类型
            job.setMapOutputValueClass(NullWritable.class);

            // 设置Reducer组件
            job.setReducerClass(ZkReducer.class);
            // 设置Reducer输出的key类型
            job.setOutputKeyClass(Teacher.class);
            // 设置Reducer输出的value类型
            job.setOutputValueClass(NullWritable.class);

            // 设置分区
//        job.setPartitionerClass(ZkPartitioner.class);
            // 设置Reducer数量
            job.setNumReduceTasks(5);

            // 设置读取器与写出器
            job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputFormatClass(DBOutputFormat.class);

            // 设置文件读取路径
            TextInputFormat.setInputPaths(job, new Path("/teacher.txt"));
            // 设置写出文件路径
//        TextOutputFormat.setOutputPath(job, new Path(output));

            // 提交作业
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /*
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new ZkDriver(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    */

    /*
    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()读取配置文件
        Configuration conf = getConf();
//        System.out.println("ZKDriver的conf：\t" + conf);
        // 获取参数
        String input = conf.get("input");
//        String output = conf.get("output");

        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主类入口
        job.setJarByClass(ZkDriver.class);
        job.setJobName("hhp_zookeeper");

        // 设置数据库连接要素
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.143:3306/briup",
                "root", "root");

        // 设置SQL语句（insert语句）
        DBOutputFormat.setOutput(job,
                "teacher",
                "name", "age");

        // 设置Mapper组件
        job.setMapperClass(ZkMapper.class);
        // 设置Mapper输出的key类型
        job.setMapOutputKeyClass(Teacher.class);
        // 设置Mapper输出的value类型
        job.setMapOutputValueClass(NullWritable.class);

        // 设置Reducer组件
        job.setReducerClass(ZkReducer.class);
        // 设置Reducer输出的key类型
        job.setOutputKeyClass(Teacher.class);
        // 设置Reducer输出的value类型
        job.setOutputValueClass(NullWritable.class);

        // 设置分区
//        job.setPartitionerClass(ZkPartitioner.class);
        // 设置Reducer数量
        job.setNumReduceTasks(5);

        // 设置读取器与写出器
        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        // 设置文件读取路径
        TextInputFormat.setInputPaths(job, new Path(input));
        // 设置写出文件路径
//        TextOutputFormat.setOutputPath(job, new Path(output));

        // 提交作业
        job.waitForCompletion(true);
        return 0;
    }
    */


}
