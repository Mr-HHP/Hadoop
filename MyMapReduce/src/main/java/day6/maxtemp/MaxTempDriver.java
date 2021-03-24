package day6.maxtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: Hadoop->day6->MaxTempDriver
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-15 16:33
 **/

// yarn jar MapReduce-1.0-SNAPSHOT.jar day6.maxtemp.MaxTempDriver -D input=data -D output=/hhp/max
public class MaxTempDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new MaxTempDriver(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public int run(String[] args) {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 获取运行参数
        String input = conf.get("input");
        String output = conf.get("output");

        /*
        conf.set("mapreduce.reduce.memory.mb","512");
        conf.set("mapreduce.map.memory.mb","512");
        conf.set("mapredcue.map.java.opts","-Xmx512m");
        conf.set("mapredcue.reduce.java.opts","-Xmx512m");
        */

        try {
            // 创建作业
            Job job = Job.getInstance(conf);
            // 设置程序运行的主类入口
            job.setJarByClass(MaxTempDriver.class);

            // 设置Mapper组件
            job.setMapperClass(MaxTempMapper.class);
            // 设置Mapper输出的key类型
            job.setMapOutputKeyClass(Text.class);
            // 设置Mapper输出的value类型
            job.setMapOutputValueClass(IntWritable.class);

            // 设置Reducer组件
            job.setReducerClass(MaxTempReducer.class);
            // 设置Reducer输出的key类型
            job.setOutputKeyClass(Text.class);
            // 设置Reducer输出的value类型
            job.setOutputValueClass(IntWritable.class);

            // 设置读取器和写出器
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // 设置待处理文件的输入路径
//            FileInputFormat.setInputPaths(job, new Path(input));
//            FileInputFormat.addInputPath(job, new Path(input));
            TextInputFormat.setInputPaths(job, new Path(input));
//            TextInputFormat.addInputPath(job, new Path(input));
            // 设置结果文件输出路径
//            FileOutputFormat.setOutputPath(job, new Path(output));
            TextOutputFormat.setOutputPath(job, new Path(output));
            // 提交作业
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
