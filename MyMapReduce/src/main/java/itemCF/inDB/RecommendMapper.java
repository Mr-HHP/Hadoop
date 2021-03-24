package itemCF.inDB;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: Hadoop->itemCF.test->RecommendMapper
 * @description: 读取初步推荐列表的Mapper类
 * @author: Mr.黄
 * @create: 2019-10-28 09:29
 **/
// 输入：用户ID  商品ID  推荐系数
// 输出：用户ID  商品ID       用户ID  商品ID  推荐系数
public class RecommendMapper extends Mapper<LongWritable, Text, UserAndProduct, Recommend> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 处理每一行数据
        String line = value.toString();
        int userID = Integer.valueOf(line.split(" ")[0].trim());
        int productID = Integer.valueOf(line.split(" ")[1].trim());
        double recommend = Double.valueOf(line.split(" ")[2].trim());
        UserAndProduct out_key = new UserAndProduct();
        Recommend out_value = new Recommend();
        // 设置输出key
        out_key.setUserID(userID);
        out_key.setProduct(productID);

        // 设置输出value
        out_value.setUserID(userID);
        out_value.setProductID(productID);
        out_value.setRecommend(recommend);

        // 输出
        context.write(out_key, out_value);
    }
}
