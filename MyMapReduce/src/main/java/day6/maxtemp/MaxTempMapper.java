package day6.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->day6.maxtemp->MaxTempMapper
 * @description: Map
 * @author: Mr.黄
 * @create: 2019-10-15 18:58
 **/
public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 此方法为Mapper的核心方法，每次处理一行内容
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切割每一行数据进行处理
        String out_key = value.toString().split(",")[0];
        int out_value = Integer.valueOf(
                value.toString().split(",")[1]);
        // 输出key和value给Reducer
        context.write(new Text(out_key),
                new IntWritable(out_value));
    }
}
