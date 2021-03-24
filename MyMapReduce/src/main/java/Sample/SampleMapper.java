package Sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->Sample->SampleMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-21 00:17
 **/
public class SampleMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        /*
        // 拆分数据
        int out_key = Integer.valueOf(value.toString().split(" ")[0]);
        String out_value = value.toString().split(" ")[1];
        */
        // 输出
        context.write(key, value);
    }
}
