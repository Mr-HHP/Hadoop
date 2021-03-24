package flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: Hadoop->controlFlow->FlowReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-18 17:06
 **/
public class FlowReducer extends Reducer<Text, Flow, Flow, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Flow flow = new Flow();
        // 将手机号相同的流量相加
        for (Flow f : values) {
            flow.setPhone(f.getPhone());
            flow.setAdder(f.getAdder());
            flow.setName(f.getName());
            sum += f.getFlow();
        }
        flow.setFlow(sum);
        // 发送
        context.write(flow, NullWritable.get());
    }
}
