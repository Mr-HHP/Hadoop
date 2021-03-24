package itemCF.step3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->itemCF.step3->Step3Mapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-26 23:04
 **/
// 输入： 用户     商品   购买次数
//      10001   20001     1
//      10001   20002     1
//      10001   20005     1
//      10001   20006     1

// 输出： 用户   商品-购买次数
public class Step3Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 处理数据
        String[] line = value.toString().split(" ");
        if (line.length != 3) return;
        int out_key = Integer.valueOf(line[0]);
        String out_vlaue = line[1] + "-" + line[2];
        // 输出
        context.write(new IntWritable(out_key), new Text(out_vlaue));
    }
}
