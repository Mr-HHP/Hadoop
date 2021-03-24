package itemCF.inDB;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->itemCF.test->UserAndProductMapper
 * @description: 处理购买列表的Mapper
 * @author: Mr.黄
 * @create: 2019-10-28 08:53
 **/
// 输入：用户ID  商品ID  购买数量
// 输出：用户ID  商品ID       用户ID  商品ID  推荐系数
public class UserAndProductMapper extends Mapper<LongWritable, Text, UserAndProduct, Recommend> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切割每一行数据
        String line = value.toString();
        int userID = Integer.valueOf(line.split(" ")[0].trim());
        int product = Integer.valueOf(line.split(" ")[1].trim());
        int buy = Integer.valueOf(line.split(" ")[2].trim());
        UserAndProduct out_key = new UserAndProduct();
        Recommend out_value = new Recommend();
        // 设置输出的key
        out_key.setUserID(userID);
        out_key.setProduct(product);

        // 设置输出的value
        out_value.setUserID(userID);
        out_value.setProductID(product);
        out_value.setRecommend(0.00);

        // 输出
        context.write(out_key, out_value);
    }
}
