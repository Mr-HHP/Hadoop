package song;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @program: Hadoop->song->SongDriver
 * @description: 统计每个人对歌曲的点击量
 * @author: Mr.黄
 * @create: 2019-10-18 09:03
 **/
public class SongDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new SongDriver(), args));
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
        job.setJarByClass(SongDriver.class);

        // 设置Mapper组件
        job.setMapperClass(SongMapper.class);
        // 设置Mapper输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出value类型
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer组件
        job.setReducerClass(SongReducer.class);
        // 设置Reducer输出key类型
        job.setOutputKeyClass(Text.class);
        // 设置Reducer输出value类型
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);


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
