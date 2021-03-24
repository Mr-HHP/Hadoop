package itemCF.step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @program: Hadoop->itemCF.step1->Step1Reducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-25 17:30
 **/
// 输入： 商品   用户-购买次数
// 输出： 商品   用户1-购买次数，用户2-购买次数，...用户n-购买次数
public class Step1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new TreeSet<String>();
        // 对不同用户对同一商品的购买进行合并
        String str = "";
        for (Text value : values) {
            set.add(value.toString());
        }
        for (String value : set) {
            str += value + ",";
        }
        // 去掉最后的逗号
        String out_value = str.substring(0, str.length() - 1);
        // 输出
        context.write(key, new Text(out_value));
    }
}
