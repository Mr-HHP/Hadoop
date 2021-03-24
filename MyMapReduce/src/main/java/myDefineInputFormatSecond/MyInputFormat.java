package myDefineInputFormatSecond;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @program: Hadoop->myDefineInputFormat->MyInputFormat
 * @description: 自定义的读取器
 * @author: Mr.黄
 * @create: 2019-10-25 09:00
 **/
public class MyInputFormat extends FileInputFormat<Student, Text> {
    @Override
    public RecordReader<Student, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        InputFormatRecordReder ifrr = new InputFormatRecordReder();
//        ifrr.initialize(split, context);
        return ifrr;
    }
}
