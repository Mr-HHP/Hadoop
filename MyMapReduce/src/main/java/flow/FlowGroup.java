package flow;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: Hadoop->controlFlow->FlowGroup
 * @description: 自定义分组，按照手机号进行分组
 * @author: Mr.黄
 * @create: 2019-10-18 17:34
 **/
public class FlowGroup extends WritableComparator {
    public FlowGroup() {
        super(Flow.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
        /*
        Flow f = (Flow) a;
        Flow f1 = (Flow) b;
        return f.getPhone().compareTo(f1.getPhone());
        */
    }
}
