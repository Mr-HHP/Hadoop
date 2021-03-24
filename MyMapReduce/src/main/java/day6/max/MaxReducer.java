package day6.max;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->day6.max->MaxReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-16 21:53
 **/
public class MaxReducer extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        // 求最高气温
        for (Text value : values) {
            Integer t = Integer.valueOf(value.toString().substring(1));
            if (t > max) {
                max = t;
            }
        }
        // 输出结果
        context.write(key, new IntWritable(max));
    }
}
