package itemCF.step3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @program: Hadoop->itemCF.step3->Step3Reducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-26 23:04
 **/
// 输入： 用户1   商品1-购买次数
//       用户1   商品2-购买次数
//        ...
public class Step3Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new TreeSet<String>();
        for (Text value : values) {
            set.add(value.toString());
        }
        // 拼接
        String out_value = "";
        for (String value : set) {
            out_value += value + ",";
        }
        // 去掉最后一个逗号
        out_value = out_value.substring(0, out_value.length() - 1);
        context.write(key, new Text(out_value));
    }
}
