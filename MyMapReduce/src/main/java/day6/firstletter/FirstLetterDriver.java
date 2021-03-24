package day6.firstletter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: Hadoop->day6.firstletter->FirstLetterDriver
 * @description: 计算首字母出现的频次，不区分大小写
 * @author: Mr.黄
 * @create: 2019-10-17 00:11
 **/
// yarn jar MapReduce-1.0-SNAPSHOT.jar day6.firstletter.FirstLetterDriver -D input=FirstLetter.txt -D output=/hhp/FirstLetter
public class FirstLetterDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new FirstLetterDriver(), args));
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
        job.setJarByClass(FirstLetterDriver.class);

        // 设置Mapper组件
        job.setMapperClass(FirstLetterMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer组件
        job.setReducerClass(FirstLetterReducer.class);
        // 设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(Text.class);

        // 设置读取器和写出器
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        TextInputFormat.setInputPaths(job, new Path(input));
        TextOutputFormat.setOutputPath(job, new Path(output));

        // 提交作业
        job.waitForCompletion(true);
        return 0;
    }
}
