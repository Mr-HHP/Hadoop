package zkMapReducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @program: Hadoop->zkMapReducer->Teacher
 * @description: 封装数据，便于存入数据库
 * @author: Mr.黄
 * @create: 2019-11-02 19:39
 **/
public class Teacher implements WritableComparable<Teacher>, DBWritable {
    private String name;
    private int age;

    @Override
    public String toString() {
        return name + "\t" + age;
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

    public Teacher(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Teacher() {
    }

    @Override
    public int compareTo(Teacher o) {
        return this.getName().compareTo(o.getName());
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.age = in.readInt();
    }

    // 数据库的序列化
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, name);
        statement.setInt(2, age);
    }

    // 数据库的反序列化
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.name = resultSet.getString("name");
        this.age = resultSet.getInt("age");
    }
}
