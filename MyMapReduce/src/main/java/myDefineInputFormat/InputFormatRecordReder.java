package myDefineInputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * @program: Hadoop->myDefineInputFormat->InputFormatRecordReder
 * @description: 自定义的读取解析器
 * @author: Mr.黄
 * @create: 2019-10-25 09:02
 **/
public class InputFormatRecordReder extends RecordReader<Student, Text> {
    // 创建一个行读取器类型的变量，这是Hadoop提供的工具类，可以一行一行读取数据
    private LineRecordReader reader = new LineRecordReader();
    // 输出的key
    private Student student;

    // 此组件的初始方法，首先会被调用，只会被调用一次 此方法作用用于初始化文件系统对象、切片对象、行读取器等
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        System.out.println("initialize测试&&&&&&&&&&&&&&&&&&&&&&");
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("nextKeyValue测试%%%%%%%%%");
        if (!reader.nextKeyValue()) return false;
        Text text = reader.getCurrentValue();
        String[] strings = text.toString().split(",");
        student = new Student();
        student.setId(Integer.valueOf(strings[0]));
        student.setName(strings[1]);
        student.setAge(Integer.valueOf(strings[2]));
        return true;
    }

    @Override
    public Student getCurrentKey() throws IOException, InterruptedException {
        System.out.println("getCurrentKey测试+++++++++++");
        return student;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        System.out.println("getCurrentValue测试============");
        return new Text("\t###");
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    // 此方法是组件结束调用后用来释放资源的
    @Override
    public void close() throws IOException {
        reader.close();
    }
}
