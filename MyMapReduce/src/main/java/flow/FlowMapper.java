package flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->controlFlow->FlowMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-17 14:08
 **/
public class FlowMapper extends Mapper<LongWritable, Text, Text,  Flow> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Flow flow = new Flow();
        // 拆分数据
        String[] line = value.toString().split(" ");
        // 手机号
        String out_key = line[0];
        // 注入对象
        flow.setPhone(line[0]);
        flow.setAdder(line[1]);
        flow.setName(line[2]);
        flow.setFlow(Integer.valueOf(line[3]));
        // 发送
        context.write(new Text(out_key), flow);
    }
}
