package day6.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->day6.wordcount->WordCountReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-16 19:31
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable i : values) {
            // 单词计数
            count+=i.get();
        }
        // 输出
        context.write(key, new IntWritable(count));
    }
}
