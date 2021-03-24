package itemCF.step2;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: Hadoop->itemCF.step2->Step3Mapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-26 13:31
 **/
public class Step2Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    // 临时存储
    private List<String> list = new ArrayList<String>();
    // 保留两位小数
    DecimalFormat df = new DecimalFormat("0.00");

    // Mapper初始化的方法，只会执行一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // 获取该文件夹下的所有文件的状态信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/hdfs/itemCF/step1"));
        // 遍历文件
        for (FileStatus fileStatus : fileStatuses) {
            // 只读取part开头的文件
            if (fileStatus.getPath().toString().indexOf("part") != -1) {
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(
                                FileSystem.get(
                                        context.getConfiguration()).open(
                                        new Path(fileStatus.getPath().toString()))));
                String line = null;
                while ((line = br.readLine()) != null) {
                    list.add(line);
                }
                // 关流
                br.close();
            }
        }
    }

    // 计算物品与物品的相似度关系（矩阵），使用余弦相似度来计算
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 按行处理用户与物品购买关系
        String left_productID = value.toString().split("\t")[0];
        String[] left_user_buy = value.toString().split("\t")[1].split(",");
        if (list == null) return;
        // 遍历集合
        for (String line : list) {
            double left_denominator = 0; // 左边分母
            double rigtht_denominator = 0; // 分右边母
            // 分割每一行数据
            String rigtht_productID = line.split("\t")[0];
            String[] rigtht_user_buy = line.split("\t")[1].split(",");
            // 计算右边分母
            for (String str : rigtht_user_buy) {
                double rigtht_but_count = Double.valueOf(str.split("-")[1]);
                rigtht_denominator += rigtht_but_count * rigtht_but_count;
            }
            rigtht_denominator = Math.sqrt(rigtht_denominator);

            // 计算相似度（矩阵相乘）
            double result = 0; // 两行相乘结果
            for (String str : left_user_buy) {
                String left_user = str.split("-")[0]; // 左边矩阵每一个用户ID
                double left_buy = Integer.valueOf(str.split("-")[1]); // 购买次数
                // 计算左边分母
                left_denominator += left_buy * left_buy;
                for (String rigth : rigtht_user_buy) {
                    String rigth_user = rigth.split("-")[0]; // 右边矩阵每一个用户ID
                    double rigth_buy = Integer.valueOf(rigth.split("-")[1]); // 购买次数
                    if (left_user.equals(rigth_user)) {
                        result += left_buy * rigth_buy;
                        break;
                    }
                }
            }
            left_denominator = Math.sqrt(left_denominator);
            double cos = result / (left_denominator * rigtht_denominator);
            // 没有相似度，直接过滤掉
            if (cos == 0) {
                continue;
            }
            // 输出： 商品1    商品1-相似度
            //       商品1    商品2-相似度
            //       ...
            int out_key = Integer.valueOf(left_productID);
            String out_value = rigtht_productID + "-" + df.format(cos);
            context.write(new IntWritable(out_key), new Text(out_value));
        }
    }
}
