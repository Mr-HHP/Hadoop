package day6.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->day6.maxtemp->MaxTempReducer
 * @description: Reducer
 * @author: Mr.黄
 * @create: 2019-10-15 18:59
 **/
public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 处理从Mapper发来的数据
        int max = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            if (value.get() > max) {
                max = value.get();
            }
        }
        context.write(key, new IntWritable(max));
    }
}
