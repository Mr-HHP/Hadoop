package myDefineInputFormat;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: Hadoop->myDefineInputFormat->Student
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-25 09:25
 **/
public class Student implements WritableComparable<Student> {
    private int id;
    private String name;
    private int age;

    public Student(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Student() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return id + "\t" + name + "\t" + age;
    }

    @Override
    public int compareTo(Student o) {
        return this.getAge() - o.getAge();
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeInt(age);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.name = in.readUTF();
        this.age = in.readInt();
    }
}
