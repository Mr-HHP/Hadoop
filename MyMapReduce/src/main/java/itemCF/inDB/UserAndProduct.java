package itemCF.inDB;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: Hadoop->itemCF.inDB->UserAndProduct
 * @description: 封装用户ID和物品ID
 * @author: Mr.黄
 * @create: 2019-10-28 09:03
 **/
public class UserAndProduct implements WritableComparable<UserAndProduct> {
    private int userID;
    private int productID;

    @Override
    public String toString() {
        return userID + " " + productID;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getProduct() {
        return productID;
    }

    public void setProduct(int product) {
        this.productID = product;
    }

    public UserAndProduct(int userID, int product) {
        this.userID = userID;
        this.productID = product;
    }

    public UserAndProduct() {
    }

    // 排序
    @Override
    public int compareTo(UserAndProduct o) {
        // 用户ID升序，用户ID相同按照物品ID升序
        int i = this.getUserID() - o.getUserID();
        if (i == 0) {
            return this.getProduct() - o.getProduct();
        }
        return i;
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userID);
        out.writeInt(productID);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.userID = in.readInt();
        this.productID = in.readInt();
    }
}
