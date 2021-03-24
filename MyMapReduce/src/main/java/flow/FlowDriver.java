package flow;

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
 * @program: Hadoop->controlFlow->FlowDriver
 * @description: 按照手机号分类，将相同手机号的流量相加，并且要区分地区
 * @author: Mr.黄
 * @create: 2019-10-17 14:08
 **/
public class FlowDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new FlowDriver(), args));
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

        /*
        conf.set("mapreduce.reduce.memory.mb","512");
        conf.set("mapreduce.map.memory.mb","512");
        conf.set("mapredcue.map.java.opts","-Xmx512m");
        conf.set("mapredcue.reduce.java.opts","-Xmx512m");
        */

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主程序入口
        job.setJarByClass(FlowDriver.class);
        job.setJobName("hhp_Flow");

        // 设置Mapper组件
        job.setMapperClass(FlowMapper.class);
        // 设置Mapper输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出value类型
        job.setMapOutputValueClass(Flow.class);

        /*
        // 设置Reducer组件
        job.setReducerClass(FlowReducer.class);
        // 设置Reducer输出key类型
        job.setOutputKeyClass(Flow.class);
        // 设置Reducer输出value类型
        job.setOutputValueClass(NullWritable.class);
        */

        // 自定义分区
        job.setPartitionerClass(FlowPartitioner.class);
        // 设置Reducer数量
        job.setNumReduceTasks(3);

        // 自定义排序
//        job.setSortComparatorClass();

        // 自定义分组
        job.setGroupingComparatorClass(FlowGroup.class);
        job.setCombinerKeyGroupingComparatorClass(FlowGroup.class);

        // 设置读取器和写出器
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        TextInputFormat.setInputPaths(job, new Path(input));
        // 设置文件输出路径
        TextOutputFormat.setOutputPath(job, new Path(output));

        // 提交作业
        job.waitForCompletion(true);


        return 0;
    }
}
