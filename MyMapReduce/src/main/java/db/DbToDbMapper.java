package db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->db->DbToDbMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-24 15:09
 **/
public class DbToDbMapper extends Mapper<LongWritable, Student, Student, NullWritable> {
    // 数据传输规则
    // DBInputFormat类读取的键是LongWritable类型
    // DB->HDFS  key：行号，从0开始  value：对象（将整行数据封装成对象）
    // HDFS->DB  key：对象（将所有数据封装成对象）  value：随意
    @Override
    protected void map(LongWritable key, Student value, Context context) throws IOException, InterruptedException {
        // 处理数据
        Student student = new Student();
        // value是从数据库读取到的每一行被封装成对象的数据
        student.setId(value.getId());
        student.setName(value.getName());
        student.setAge(value.getAge());
        // 输出
        context.write(student, NullWritable.get());
    }
}
