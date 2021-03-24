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
对用户的链接键进行排序
 */
public class SecondStage extends Configured implements Tool {
    static class SecondStageMapper
            extends Mapper<LongWritable, Text, IntWritable,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[] =value.toString().split(",");
            context.write(new IntWritable(Integer.parseInt(strs[1])),
                    new Text(strs[1]+","+strs[0]));
        }
    }
    static class SecondStageReduce
            extends Reducer<IntWritable,Text, NullWritable,Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t:values){
                context.write(NullWritable.get(),t);
            }
        }
    }
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");

        Job job= Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("user_sort");

        job.setMapperClass(SecondStageMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(SecondStageReduce.class);
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
                            new SecondStage(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
