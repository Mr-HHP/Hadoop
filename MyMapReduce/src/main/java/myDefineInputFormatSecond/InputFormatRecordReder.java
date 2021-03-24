package myDefineInputFormatSecond;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * @program: Hadoop->myDefineInputFormat->InputFormatRecordReder
 * @description: 自定义的读取解析器
 * @author: Mr.黄
 * @create: 2019-10-25 09:02
 **/
public class InputFormatRecordReder extends RecordReader<Student, Text> {
    // 创建分片信息
    private FileSplit fs;
    // 创建一个行读取器类型的变量，这是Hadoop提供的工具类，可以一行一行读取数据
    private LineReader reader;
    // 输出的key
    private Student student;

    // 此组件的初始方法，首先会被调用，只会被调用一次 此方法作用用于初始化文件系统对象、切片对象、行读取器等
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化切片
        fs = (FileSplit) split;
        // 获取地址
        Path path = fs.getPath();
        // 创建环境变量
        Configuration conf = new Configuration();
        // 获取文件系统
        FileSystem system = path.getFileSystem(conf);
        // 获取输入流
        InputStream in = system.open(path);
        // 初始化行读取器，行读取器是Hadoop提供的工具类，可以一行一行的读取数据
        reader = new LineReader(in);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 创建临时存放数据的空间
        Text text = new Text();
        int length = reader.readLine(text);
        if (length == 0) {
            // 读取的数据长度为0，代表数据已经读完，终止nextKeyValue()方法
            return false;
        }
        student = new Student();
        String[] strings = text.toString().split(",");
        student.setId(Integer.valueOf(strings[0]));
        student.setName(strings[1]);
        student.setAge(Integer.valueOf(strings[2]));
        return true;
    }

    @Override
    public Student getCurrentKey() throws IOException, InterruptedException {
        return student;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text("&&&");
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    // 此方法是组件结束调用后用来释放资源的
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader = null;
        }
    }
}
