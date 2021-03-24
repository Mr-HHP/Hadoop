package itemCF.step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->itemCF.step1->Step1Mapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-25 17:30
 **/
// 输入： 用户     商品   购买次数
//      10001   20001     1
//      10001   20002     1
//      10001   20005     1
//      10001   20006     1

    // 输出： 商品   用户-购买次数
public class Step1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 处理数据
        String[] line = value.toString().split(" ");
        // 过滤掉错误数据
        if (line.length != 3) return;
        // 商品编号
        int out_key = Integer.valueOf(line[1]);
        // 用户编号-购买次数
        String out_value = line[0] + "-" + line[2];
        // 输出
        context.write(new IntWritable(out_key), new Text(out_value));
    }
}
