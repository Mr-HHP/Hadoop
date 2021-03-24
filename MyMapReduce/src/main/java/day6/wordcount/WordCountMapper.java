package day6.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->day6.wordcount->WordCountMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-16 19:30
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 数据拆分
        String[] words = value.toString().split(" ");
        if (words != null && words.length > 0) {
            for (String out_key : words) {
                // 发送给Reducer
                context.write(new Text(out_key), new IntWritable(1));
            }
        }
    }
}
