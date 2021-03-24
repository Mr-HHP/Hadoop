package day6.email;

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

/**
 * @program: Hadoop->day6.email->EmailDriver
 * @description: 统计每种邮箱出现的次数
 * @author: Mr.黄
 * @create: 2019-10-17 09:08
 **/
// yarn jar MapReduce-1.0-SNAPSHOT.jar day6.email.EmailDriver -D input=email.txt -D output=/hhp/email
public class EmailDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new EmailDriver(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 获取参数
        String input = conf.get("input");
        String output = conf.get("output");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主程序入口
        job.setJarByClass(EmailDriver.class);
        job.setJobName("hhp_Email");

        // 设置Mapper组件
        job.setMapperClass(EmailMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reducer组件
        job.setReducerClass(EmailReducer.class);
        // 设置Reducer输出可以的类型
        job.setOutputKeyClass(Text.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(IntWritable.class);

        // 设置读取器和写出器
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        TextInputFormat.setInputPaths(job, new Path(input));
        // 设置输出文件路径
        TextOutputFormat.setOutputPath(job, new Path(output));

        // 提交作业
        job.waitForCompletion(true);
        return 0;
    }
}
