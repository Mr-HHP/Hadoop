package day6.wordcount;

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
 * @program: Hadoop->day6.wordcount->WordCountDriver
 * @description: 单词统计
 * @author: Mr.黄
 * @create: 2019-10-16 19:30
 **/
//yarn jar MapReduce-1.0-SNAPSHOT.jar day6.wordcount.WordCountDriver -D input=data -D output=/hhp/word_count
public class WordCountDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new WordCountDriver(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 获取参数
        String input = conf.get("input");
        String output = conf.get("output");

        try {
            // 创建作业
            Job job = Job.getInstance(conf);
            // 设置主程序入口
            job.setJarByClass(WordCountDriver.class);
            job.setJobName("hhp_wordcount");

            // 设置Mapper组件
            job.setMapperClass(WordCountMapper.class);
            // 设置Mapper输出key类型
            job.setMapOutputKeyClass(Text.class);
            // 设置Mapper输出value类型
            job.setMapOutputValueClass(IntWritable.class);

            // 设置Reducer组件
            job.setReducerClass(WordCountReducer.class);
            // 设置Reducer输出key类型
            job.setOutputKeyClass(Text.class);
            // 设置Reducer输出value类型
            job.setOutputValueClass(IntWritable.class);

            // 设置读取器和写出器
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // 设置读取文件路径
            TextInputFormat.setInputPaths(job, new Path(input));
            // 设置写出文件路径
            TextOutputFormat.setOutputPath(job, new Path(output));
            // 提交作业
            job.waitForCompletion(true);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
