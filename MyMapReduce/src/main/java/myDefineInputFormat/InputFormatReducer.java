package myDefineInputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->myDefineInputFormat->InputFormatReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-25 09:00
 **/
public class InputFormatReducer extends Reducer<Student, Text, Student, Text> {
    @Override
    protected void reduce(Student key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, new Text(value));
        }
    }
}
