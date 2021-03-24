package flow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: Hadoop->controlFlow->Flow
 * @description: 手机流量类
 * @author: Mr.黄
 * @create: 2019-10-17 14:05
 **/

// 实现序列化，并重写排序方法
public class Flow implements WritableComparable<Flow> {
    private String phone;
    private String adder;
    private String name;
    private int flow;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAdder() {
        return adder;
    }

    public void setAdder(String adder) {
        this.adder = adder;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFlow() {
        return flow;
    }

    public void setFlow(int flow) {
        this.flow = flow;
    }

    @Override
    public String toString() {
        return "phone：" + phone +
                "\tadder：'" + adder +
                "\tname：" + name +
                "\tcontrolFlow：" + flow;
    }

    // 进行序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeUTF(adder);
        out.writeUTF(name);
        out.writeInt(flow);

    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.adder = in.readUTF();
        this.name = in.readUTF();
        this.flow = in.readInt();
    }

    // 按照流量升序
    @Override
    public int compareTo(Flow o) {
        return this.getFlow() - o.getFlow();
    }
}
