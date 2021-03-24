package day6.firstletter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->day6.firstletter->FirstLetterReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 00:12
 **/
public class FirstLetterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String words = "";
        // 统计首字母次数
        for (Text value : values) {
            count++;
            // 将出现的单词进行拼接
            words = words + "-" + value.toString();
        }
        // 拼接次数
        String out_value = count + "\t" + words;
        // 输出
        context.write(key, new Text(out_value));
    }
}
