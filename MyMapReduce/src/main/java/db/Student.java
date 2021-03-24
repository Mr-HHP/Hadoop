package db;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @program: Hadoop->db->Student
 * @description: 学生类，封装学生信息
 * @author: Mr.黄
 * @create: 2019-10-24 15:13
 **/
public class Student implements WritableComparable<Student>, DBWritable {
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
        return this.getId() - o.getId();
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

    // 数据库的序列化
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, id);
        statement.setString(2, name);
        statement.setInt(3, age);
    }

    // 数据库的反序列化
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        if (resultSet == null) return;
        this.id = resultSet.getInt("id");
        this.name = resultSet.getString("name");
        this.age = resultSet.getInt("age");
    }
}
