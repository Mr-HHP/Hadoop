package db;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Teacher
        implements WritableComparable<Teacher>, DBWritable {
//    private long id;
    private String name;
    private int age;

    public int compareTo(Teacher o) {
        return this.getAge() - o.getAge();
    }


    @Override
    public String toString() {
        return name+","+age;
    }

    public Teacher() {
    }

    public Teacher(long id, String name, int age) {
//        this.id = id;
        this.name = name;
        this.age = age;
    }

    /*
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
     */

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

    public void write(DataOutput out) throws IOException {
//        out.writeLong(id);
        out.writeUTF(name);
        out.writeInt(age);
    }

    public void readFields(DataInput in) throws IOException {
//        this.id=in.readLong();
        this.name=in.readUTF();
        this.age=in.readInt();
    }

    public void write(
            PreparedStatement preparedStatement)
            throws SQLException {
//        preparedStatement.setLong(1,id);
        preparedStatement.setString(1,name);
        preparedStatement.setInt(2,age);
    }

    /*
    select name,age,id
    from teacher
     */
    public void readFields(ResultSet resultSet)
            throws SQLException {
        if(resultSet==null)return;
//        this.id=resultSet.getLong("id");
        //this.name=resultSet.getString("name");
        this.name=resultSet.getString("name");
        this.age=resultSet.getInt("age");
    }
}
