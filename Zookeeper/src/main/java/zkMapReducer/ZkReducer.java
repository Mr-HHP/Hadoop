package zkMapReducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->zkMapReducer->ZkReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-11-02 19:38
 **/
public class ZkReducer extends Reducer<Teacher, NullWritable, Teacher, NullWritable> {
    @Override
    protected void reduce(Teacher key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 原样输出
        context.write(key, NullWritable.get());
    }
}
