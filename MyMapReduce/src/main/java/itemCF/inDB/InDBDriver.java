package itemCF.inDB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: Hadoop->itemCF.test->InDBDriver
 * @description: 数据录入数据库
 * @author: Mr.黄
 * @create: 2019-10-28 08:52
 **/
public class InDBDriver {
    public static Job step5(Configuration conf) throws IOException {
        // 使用getConf()方法读取配置文件
//        Configuration conf = getConf();

        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主类
        job.setJarByClass(InDBDriver.class);
        job.setJobName("hhp_inDB");

        // 设置连接要素
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.140:3306/briup",
                "root", "root");

        /*
        insert into teacher(userID,productID,recommend) values(?,?,?)
         */
        DBOutputFormat.setOutput(job, "recommend",
                "userID", "productID", "recommend");


        // 多个Mapper
        // 设置Mapper组件
        MultipleInputs.addInputPath(
                job, new Path("/matrix.txt"),
                TextInputFormat.class, UserAndProductMapper.class);
        MultipleInputs.addInputPath(
                job, new Path("itemCF/step4/part-r-00000"),
                TextInputFormat.class, RecommendMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(UserAndProduct.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(Recommend.class);

        // 设置Reducer组件
        job.setReducerClass(InDBReducer.class);
        // 设置Recuder输出key的类型
        job.setOutputKeyClass(Recommend.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(NullWritable.class);

        // 设置读取器和写出器
        job.setOutputFormatClass(DBOutputFormat.class);
        // 提交作业
//        job.waitForCompletion(true);
        return job;
    }

    /*
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new InDBDriver(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();

        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主类
        job.setJarByClass(InDBDriver.class);
        job.setJobName("hhp_inDB");

        // 设置连接要素
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.140:3306/briup",
                "root", "root");


//        insert into teacher(userID,productID,recommend) values(?,?,?)
        DBOutputFormat.setOutput(job, "recommend",
                "userID", "productID", "recommend");


        // 多个Mapper
        // 设置Mapper组件
        MultipleInputs.addInputPath(
                job, new Path("/matrix.txt"),
                TextInputFormat.class, UserAndProductMapper.class);
        MultipleInputs.addInputPath(
                job, new Path("/test/part-r-00000"),
                TextInputFormat.class, RecommendMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(UserAndProduct.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(Recommend.class);

        // 设置Reducer组件
        job.setReducerClass(InDBReducer.class);
        // 设置Recuder输出key的类型
        job.setOutputKeyClass(Recommend.class);
        // 设置Reducer输出value的类型
        job.setOutputValueClass(NullWritable.class);

        // 设置读取器和写出器
        job.setOutputFormatClass(DBOutputFormat.class);
        // 提交作业
        job.waitForCompletion(true);

        return 0;
    }
    */

}
