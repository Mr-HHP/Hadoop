package db;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->db->DbToDbReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-24 15:12
 **/
public class DbToDbReducer extends Reducer<Student, NullWritable, Student, NullWritable> {
    @Override
    protected void reduce(Student key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 一个key就是一行数据，只是被封装了起来
        Student student = new Student();
        student.setId(key.getId());
        student.setName(key.getName());
        student.setAge(key.getAge());
        // 输出
        context.write(student, NullWritable.get());
    }
}
