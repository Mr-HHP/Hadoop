package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
对歌曲songs.txt的链接键排序
 */
public class FirstStage extends Configured implements Tool {
    /*
    1,song1,33
    2,song2,44
    3,song3,22
    4,song4,35
    TextINputFormat->map
       key      value
        0        1,song1,33
        10        2,song2,44
                3,song3,22
                4,song4,35

    map->输出
    key         value
     1           1,song1,33
     2            2,song2,44
     3            3,song3,22
     4            4,song4,35
     */
    static class FirstStageMapper
            extends Mapper<LongWritable, Text, IntWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] strs=value.toString().split(",");
            context.write(new IntWritable(Integer.parseInt(strs[0])),
                    value);
        }
    }
    /*
    分区：1个

    分组
     key         value
     1           1,song1,33
     2            2,song2,44
     3            3,song3,22
     4            4,song4,35
    排序：
     key         value
     1           1,song1,33
     2            2,song2,44
     3            3,song3,22
     4            4,song4,35
     合并
    key value
    1           [(1,song1,33)]
    2            [2,song2,44]
     3            [3,song3,22]
     4            [4,song4,35]
     */
    static class FirstStageReduce
            extends Reducer<IntWritable,Text, NullWritable,Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val:values){
                context.write(NullWritable.get(),val);
            }
        }
    }
    /*
    reduce->
    1,song1,33
    2,song2,44
    3,song3,22
    4,song4,35
     */
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");

        Job job= Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("song_sort");

        job.setMapperClass(FirstStageMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(FirstStageReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(input));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));
        TextOutputFormat.setCompressOutput(job,true);
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) {
        try {
            System.exit(
                    new ToolRunner().run(
                            new FirstStage(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
