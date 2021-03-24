package nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NlineInputformatTest extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");

        Job job= Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("word_count");

        job.setMapperClass(TokenCounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /*
        NLineInputFormat 对数据文件一行一行的处理
        每一行产生一个键值对，键为每一行的偏移位置，值为整行内容
        作用：基于文件中行进行分片，每一个分片对应一个map
        通常针对的是不能分片的文件（类似gzip压缩文件）
        setNumLinesPerSplit 第二个参数指定分片的标准
         */
        NLineInputFormat.setNumLinesPerSplit(job,2);
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job,new Path(input));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) {
        try {
            System.exit(
                    new ToolRunner().run(
                            new NlineInputformatTest(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
