package defineFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class YeaStationMaxTem extends Configured implements Tool {
    static class YeaStationMaxTemMapper extends Mapper<YearStation, IntWritable, Text, IntWritable> {
        @Override
        protected void map(YearStation key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), value);
        }
    }

    static class YeaStationMaxTemReduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            for (IntWritable n : values) {
                if (max < n.get()) max = n.get();
            }
            context.write(key, new IntWritable(max));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");
        String input = conf.get("input");
        String output = conf.get("output");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("define");

        job.setMapperClass(YeaStationMaxTemMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(YeaStationMaxTemReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(YearStationInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        YearStationInputFormat.addInputPath(job, new Path(input));
        TextOutputFormat.setOutputPath(job, new Path(output));


        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) {
        try {
            System.exit(
                    new ToolRunner().run(
                            new YeaStationMaxTem(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}