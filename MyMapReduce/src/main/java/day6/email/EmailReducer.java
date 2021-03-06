package day6.email;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->day6.email->EmailReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 09:09
 **/
public class EmailReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 处理数据
        int count = 0;
        for (IntWritable value : values) {
            count+=value.get();
        }
        // 输出
        context.write(key, new IntWritable(count));
    }
}
