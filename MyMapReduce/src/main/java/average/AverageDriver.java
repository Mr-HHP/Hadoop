package average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: Hadoop->average->AverageDriver
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 09:54
 **/
// yarn jar MapReduce-1.0-SNAPSHOT.jar average.AverageDriver -D input=hhp/average -D output=/hhp/average
public class AverageDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new AverageDriver(), args));
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
        job.setJarByClass(AverageDriver.class);
        job.setJobName("hhp_average");

        // 设置Mapper组件
        job.setMapperClass(AverageMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(DoubleWritable.class);

        // 设置Reducer组件
        job.setReducerClass(AverageReducer.class);
        // 设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(DoubleWritable.class);

        // 设置读取器和写出器
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        TextInputFormat.setInputPaths(job, new Path(input));
        // 设置写出文件路径
        TextOutputFormat.setOutputPath(job, new Path(output));
        // 提交作业
        job.waitForCompletion(true);
        return 0;
    }
}
