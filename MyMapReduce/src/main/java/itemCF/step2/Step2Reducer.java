package itemCF.step2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @program: Hadoop->itemCF.step2->Step3Reducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-26 13:31
 **/
// 输入： 商品1    商品1-相似度
//       商品1    商品2-相似度
public class Step2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new TreeSet<String>();
        // 拼接数据
        String out_value = "";
        for (Text value : values) {
            set.add(value.toString());
        }
        for (String value : set) {
            out_value += value + ",";
        }
        // 去掉最后一个逗号
        out_value = out_value.substring(0, out_value.length() - 1);
        // 输出：  商品    商品1-相似度，商品2-相似度，...商品n-相似度
        context.write(key, new Text(out_value));
    }
}
