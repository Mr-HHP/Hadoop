package zkMapReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->zkMapReducer->ZkMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-11-02 19:38
 **/
public class ZkMapper extends Mapper<LongWritable, Text, Teacher, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切割数据
        String line = value.toString();
        String name = line.split("\t")[0].trim();
        int age = Integer.valueOf(line.split("\t")[1].trim());
        Teacher teacher = new Teacher(name, age);
        // 输出
        context.write(teacher, NullWritable.get());
    }
}
