package myDefineInputFormatSecond;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->myDefineInputFormat->InputFormatMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-25 08:58
 **/
public class InputFormatMapper extends Mapper<Student, Text, Student, Text> {
    @Override
    protected void map(Student key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
