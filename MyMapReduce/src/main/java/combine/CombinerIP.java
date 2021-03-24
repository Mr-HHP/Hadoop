package combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CombinerIP extends Configured implements Tool {
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

        //job.setNumReduceTasks(0);

//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);
        /*
        CombineTextInputFormat 处理文本文件，每一行产生一个键值对
        每一行的偏移位置是key，整行内容是值
        作用：CombineTextInputFormat整个小文件，多个小文件可以用一个map处理
         */
        job.setInputFormatClass(CombineTextInputFormat.class);
        
        //第二参数表示字节
        CombineTextInputFormat.setMaxInputSplitSize(job,102400);
        CombineTextInputFormat.setMinInputSplitSize(job,40000);
        CombineTextInputFormat.addInputPath(job,new Path(input));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) {
        try {
            System.exit(
                    new ToolRunner().run(
                            new CombinerIP(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}