package day6.max;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->day6.max->MaxMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-16 21:52
 **/
public class MaxMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分数据
        String out_key = value.toString().substring(8, 12);
        String out_value = value.toString().substring(18, 23);
        // 发送数据
        context.write(new Text(out_key), new Text(out_value));
    }
}
