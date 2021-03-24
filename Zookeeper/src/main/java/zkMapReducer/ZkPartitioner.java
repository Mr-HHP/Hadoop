package zkMapReducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: Hadoop->zkMapReducer->ZkPartitioner
 * @description: 设置分区
 * @author: Mr.黄
 * @create: 2019-11-02 23:13
 **/
public class ZkPartitioner extends Partitioner<Teacher, NullWritable> {
    @Override
    public int getPartition(Teacher teacher, NullWritable nullWritable, int numPartitions) {
        if (teacher.getAge() < 20) {
            return 0;
        } else if (teacher.getAge() >= 20 && teacher.getAge() < 40) {
            return 1;
        } else if (teacher.getAge() >= 40 && teacher.getAge() < 60) {
            return 2;
        } else if (teacher.getAge() >= 60 && teacher.getAge() < 80) {
            return 3;
        } else {
            return 4;
        }
    }
}
