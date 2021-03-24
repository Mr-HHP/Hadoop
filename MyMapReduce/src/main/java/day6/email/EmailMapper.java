package day6.email;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->day6.email->EmailMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 09:09
 **/
public class EmailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分数据
        String out_key = value.toString().split("@")[1].split("\\.")[0];
        // 输出给Reducer
        context.write(new Text(out_key), new IntWritable(1));
    }
}
