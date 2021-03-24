package itemCF.step4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @program: Hadoop->itemCF.step4->Step4Reducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-26 23:30
 **/
public class Step4Reducer extends Reducer<IntWritable, Text, Text, NullWritable> {
    // 输入：商品ID    用户ID-推荐系数
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new TreeSet<String>();
        // 给value排序
        for (Text value : values) {
            set.add(value.toString());
        }
        // 拼接数据
        for (String value : set) {
            String userID = value.split("-")[0].trim();
            String reimport = value.split("-")[1].trim();
            String out_key = userID + " " + key + " " + reimport;
            context.write(new Text(out_key), NullWritable.get());
        }
        // 输出：  用户ID  商品ID  推荐系数
    }
}
