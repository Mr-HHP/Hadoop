package day6.firstletter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->day6.firstletter->FirstLetterMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 00:12
 **/
public class FirstLetterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切割数据，取出单词
        String[] words = value.toString().split(" ");
        for (String word : words) {
            // 取出首字母，并使用toUpperCase()方法将单词转换成大写
            String out_key = word.substring(0, 1).toUpperCase();
            // 输出
            context.write(new Text(out_key), new Text(word));
        }
    }
}
