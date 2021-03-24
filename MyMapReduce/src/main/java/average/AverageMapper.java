package average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->average->AverageMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 11:00
 **/
public class AverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分数据
        String out_key = value.toString().split(" ")[0];
        int score = Integer.valueOf(value.toString().split(" ")[1]);
        // 发送
        context.write(new Text(out_key), new DoubleWritable(score));

    }
}
