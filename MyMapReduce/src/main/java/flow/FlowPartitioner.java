package flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: Hadoop->controlFlow->FlowPartitioner
 * @description: 实现自定义分区
 * @author: Mr.黄
 * @create: 2019-10-18 17:07
 **/
public class FlowPartitioner extends Partitioner<Text, Flow> {
    // 前两个参数：map方法输出的键值对类型
    // 第三个参数：分区数
    @Override
    public int getPartition(Text text, Flow flow, int numPartitions) {
        // 按照城市进行分区
        if ("sh".equals(flow.getAdder().trim())) {
            return 0;
        } else if ("bj".equals(flow.getAdder().trim())) {
            return 1;
        }
        return 2;
    }
}
