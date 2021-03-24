package Sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @program: Hadoop->Sample->SampleDriver
 * @description: 采样文件测试
 * @author: Mr.黄
 * @create: 2019-10-21 00:18
 **/
public class SampleDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new SampleDriver(), args));
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

        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主方法入口
        job.setJarByClass(SampleDriver.class);
        job.setJobName("hhp_Smaple");

        // 设置Mapper组件
        job.setMapperClass(SampleMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer组件
        job.setReducerClass(SampleReducer.class);
        // 设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(Text.class);

        // 设置读取器和写出器
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        SequenceFileInputFormat.setInputPaths(job, new Path(input));
        // 设置输出文件路径
        TextOutputFormat.setOutputPath(job, new Path(output));

        // 设置分区
        job.setPartitionerClass(TotalOrderPartitioner.class);
        // 设置Reducer个数
        job.setNumReduceTasks(3);

        // 设置采样器
        // 指定的键值类型是map读取的键值类型
        // 第一个参数：采样因子  （0-1）
        // 第二个参数：采样的个数
        // 第三个参数：分区数
        // 注意：采样的第一个参数和第二个参数只要一个达到标准，采样就停止

        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler
                        .RandomSampler<Text, Text>(0.8, 100, 5);
        // 在客户端执行采样器，生成采样文件（分区文件）
        InputSampler.writePartitionFile(job, sampler);
        // 获取文件路径
        String path = TotalOrderPartitioner.getPartitionFile(conf);
        System.out.println("采样文件：" + path);

        // 把采样文件（分区文件）发送给所有map所在计算机的节点
//        String filename =
        // 自己生成采样文件，分发给从节点，经测试，失败
//        job.addCacheFile(new URI("/user/huzl/hhp/sample"));
        job.addCacheFile(new URI(path));

        // 提交作业
        job.waitForCompletion(true);
        return 0;
    }
}
