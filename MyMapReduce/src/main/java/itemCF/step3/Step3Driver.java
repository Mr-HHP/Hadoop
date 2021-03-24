package itemCF.step3;

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
 * @program: Hadoop->itemCF.step3->Step3Driver
 * @description: 转换用户与物品的购买关系，将用户ID作为key，“商品ID-购买次数”作为value
 * @author: Mr.黄
 * @create: 2019-10-26 23:00
 **/
public class Step3Driver {
    public static Job step3(Configuration conf) throws IOException {
        // 使用getConf()方法读取配置文件
//        Configuration conf = getConf();
        // 获取参数
//        String input = conf.get("/matrix.txt");
//        String output = conf.get("itemCF/step3");

        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主类
        job.setJarByClass(Step3Driver.class);
        job.setJobName("hhp_Step3");

        // 设置Mapper组件
        job.setMapperClass(Step3Mapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(IntWritable.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer组件
        job.setReducerClass(Step3Reducer.class);
        // 设置Recuder输出key的类型
        job.setOutputKeyClass(IntWritable.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(Text.class);

        // 设置读取器和写出器
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        TextInputFormat.setInputPaths(job, new Path("/matrix.txt"));
        // 设置写出文件路径
        TextOutputFormat.setOutputPath(job, new Path("itemCF/step3"));
        // 提交作业
//        job.waitForCompletion(true);
        return job;
    }

    /*
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new Step3Driver(), args));
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
        // 设置主类
        job.setJarByClass(Step3Driver.class);
        job.setJobName("hhp_Step3");

        // 设置Mapper组件
        job.setMapperClass(Step3Mapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(IntWritable.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer组件
        job.setReducerClass(Step3Reducer.class);
        // 设置Recuder输出key的类型
        job.setOutputKeyClass(IntWritable.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(Text.class);

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
    */
}
