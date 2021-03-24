package itemCF.inDB;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @program: Hadoop->itemCF.inDB->Recommend
 * @description: 封装用户和物品的推荐系数
 * @author: Mr.黄
 * @create: 2019-10-27 22:38
 **/
public class Recommend implements WritableComparable<Recommend>, DBWritable {
    private int userID;
    private int productID;
    private double recommend;

    @Override
    public String toString() {
        return userID + " " + productID + " " + recommend;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getProductID() {
        return productID;
    }

    public void setProductID(int productID) {
        this.productID = productID;
    }

    public double getRecommend() {
        return recommend;
    }

    public void setRecommend(double recommend) {
        this.recommend = recommend;
    }

    public Recommend(int userID, int productID, double recommend) {
        this.userID = userID;
        this.productID = productID;
        this.recommend = recommend;
    }

    public Recommend() {
    }

    // 排序
    @Override
    public int compareTo(Recommend o) {
        // 用户ID升序，用户ID相同按照物品ID升序
        int i = this.getUserID() - o.getUserID();
        if (i == 0) {
            return this.getProductID() - o.getProductID();
        }
        return i;
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userID);
        out.writeInt(productID);
        out.writeDouble(recommend);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.userID = in.readInt();
        this.productID = in.readInt();
        this.recommend = in.readDouble();
    }

    // 数据库的序列化
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, userID);
        statement.setInt(2, productID);
        statement.setDouble(3, recommend);
    }

    // 数据库的反序列化
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        if (resultSet == null) return;
        this.userID =  resultSet.getInt("userID");
        this.productID = resultSet.getInt("productID");
        this.recommend = resultSet.getDouble("recommend");
    }
}
