package average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->average->AverageReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 11:00
 **/
public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // 处理数据
        double score = 0;
        int count = 0;
        for (DoubleWritable value : values) {
            score+=value.get();
            count++;
        }
        // 求平均分
        double average = score / count;
        // 输出
        context.write(key, new DoubleWritable(average));
    }
}
