package itemCF.step4;

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
 * @program: Hadoop->itemCF.step4->Step4Mapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-26 23:29
 **/
public class Step4Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    // 缓存用户和物品的购买关系列表
    List<String> list = new ArrayList<String>();
    // 输出保留小数位数
    DecimalFormat df = new DecimalFormat("0.00");

    // 格式：  用户ID    商品1-购买次数，商品2-购买次数...
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取HDFS文件系统
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // 获取缓存文件目录下的所有文件信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/hdfs/itemCF/step3"));
        // 遍历文件
        for (FileStatus fileStatus : fileStatuses) {
            // 只缓存part开头的文件
            if (fileStatus.getPath().toString().indexOf("part") != -1) {
                // 创建读取流
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(
                                fs.open(fileStatus.getPath())));
                String line = null;
                while ((line = br.readLine()) != null) {
                    // 存入缓存
                    list.add(line);
                }
                // 关闭流
                br.close();
            }
        }
    }

    // 输入：  商品    商品1-相似度，商品2-相似度，...商品n-相似度
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (list == null) return;
        // 拆分读取到的一行数据
        int out_key = Integer.valueOf(value.toString().split("\t")[0]);
        String[] left_productID_similarity = value.toString().split("\t")[1].split(",");
        // 遍历集合处理缓存数据
        // 数据格式： 用户1   商品1-购买次数，商品2-购买次数...
        for (String line : list) {
            String rigth_userID = line.split("\t")[0];
            String[] rigth_productID_buy = line.split("\t")[1].split(",");

            // 计算相似度（矩阵相乘）
            double result = 0; // 两行相乘结果
            // 列与列相乘
            for (String s : left_productID_similarity) {
                String left_productID = s.split("-")[0]; // 左边列表的商品ID
                double similarity = Double.valueOf(s.split("-")[1]); // 商品的相似度
                for (String str : rigth_productID_buy) {
                    String rigth_productID = str.split("-")[0]; // 右边商品的ID
                    double buy_count = Double.valueOf(str.split("-")[1]); // 商品购买次数
                    // 计算分子
                    if (left_productID.equals(rigth_productID)) {
                        result += similarity * buy_count;
                        break;
                    }
                }
            }
            if (result == 0) {
                continue;
            }
            String out_value = rigth_userID + "-" + df.format(result);
            // 输出： 商品ID    用户ID-推荐系数
            context.write(new IntWritable(out_key), new Text(out_value));

        }
    }
}
