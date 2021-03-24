package itemCF.inDB;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @program: Hadoop->itemCF.test->InDBReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-28 08:54
 **/
// 输入：用户ID  商品ID       用户ID  商品ID  推荐系数
// 输出：用户ID  商品ID  推荐系数
public class InDBReducer extends Reducer<UserAndProduct, Recommend, Recommend, NullWritable> {
    @Override
    protected void reduce(UserAndProduct key, Iterable<Recommend> values, Context context) throws IOException, InterruptedException {
        Iterator<Recommend> iterator = values.iterator();
        Recommend out_key = iterator.next();
        // 如果有下一个，则说明用户已经购买过商品
        // 没有下一个，则说明用户没有购买过商品
        if (!iterator.hasNext()) {
            context.write(out_key, NullWritable.get());
        }

    }
}
