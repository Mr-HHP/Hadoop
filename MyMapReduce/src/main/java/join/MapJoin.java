package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MapJoin extends Configured implements Tool {
    static class MapJoinMapper
            extends Mapper<Text, TupleWritable, IntWritable, Text> {
        private StringBuffer sb = new StringBuffer();

        @Override
        protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
            for (Writable t : value) {
                sb.append(t.toString()).append("#");
            }
            sb.setLength(sb.length() - 1);
            context.write(new IntWritable(Integer.parseInt(key.toString())),
                    new Text(sb.toString()));
            sb.setLength(0);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String songs_input = conf.get("song_input");
        String user_input = conf.get("user_input");
        String output = conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");


        //设置compisteInputFormat的表达式
        String com_ex = CompositeInputFormat.compose("inner",
                KeyValueTextInputFormat.class,
                new Path(songs_input), new Path(user_input));
        conf.set("mapreduce.join.expr", com_ex);

        // 创建作业
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("map_join");

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置读取器和写出器
        job.setInputFormatClass(CompositeInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置读取文件路径
        KeyValueTextInputFormat.addInputPath(job,
                new Path(songs_input));
        KeyValueTextInputFormat.addInputPath(job,
                new Path(user_input));

        // 设置输出文件路径
        TextOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : -1;

    }

    public static void main(String[] args) {
        try {
            System.exit(new ToolRunner().run(new MapJoin(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
