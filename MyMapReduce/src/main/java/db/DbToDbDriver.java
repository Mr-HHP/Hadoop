package db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: Hadoop->db->DbToDbDriver
 * @description: 在Hadoop集群上，利用yarn平台，实现数据库到库的传输
 * @author: Mr.黄
 * @create: 2019-10-24 15:10
 **/



// 记得测试使用采样器进行全局排序，采样的数据类型是Text类型

public class DbToDbDriver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new DbToDbDriver(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 不需要获取参数，因为是从数据库中读取然后输出到数据库

        conf.set("mapreduce.reduce.memory.mb","512");
        conf.set("mapreduce.map.memory.mb","512");
        conf.set("mapredcue.map.java.opts","-Xmx512m");
        conf.set("mapredcue.reduce.java.opts","-Xmx512m");

        // 创建作业
        Job job = Job.getInstance(conf);
        // 设置主方法入口
        job.setJarByClass(DbToDbDriver.class);
        job.setJobName("hhp_DbToDb");


        // 设置DBInputFormat读取器中数据库的连接要素
        // 参数1：获取集群配置文件
        // 参数2：获取驱动
        // 参数3：获取连接
        // 参数4：数据库用户
        // 参数5：用户密码
        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.140:3306/briup",
                "root", "root");

        // 测试，检查两个对象是否一样
        System.out.println("conf：\t" + conf);
        System.out.println("job.getConfiguration：\t" + job.getConfiguration());

        /*
        select id,name,age
        from teacher
        where id<4
        order by name
         */
        // 设置读取器查询的SQL语句
        DBInputFormat.setInput(job, Student.class,
                "student",
                "1=1", "id",
                "id", "name", "age");

        // 设置DBOutputFormat写出器的连接要素
//        DBConfiguration.configureDB();

        /*
        insert into teacher(name,age) values(?,?)
         */
        // 设置写出器插入的SQL语句
        DBOutputFormat.setOutput(job, "stu",
                "id", "name", "age");



        // 设置Mapper组件
        job.setMapperClass(DbToDbMapper.class);
        // 设置Mapper输出key的类型
        job.setMapOutputKeyClass(Student.class);
        // 设置Mapper输出value的类型
        job.setMapOutputValueClass(NullWritable.class);

        // 设置Reducer组件
        job.setReducerClass(DbToDbReducer.class);
        // 设置Reducer输出的key类型
        job.setOutputKeyClass(Student.class);
        // 设置Reducer输出的value类型
        job.setOutputValueClass(NullWritable.class);

        // 设置读取器和写出器
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        // 不需要设置读取和写出文件位置，因为数据都是在数据库里

        // 提交作业
        job.waitForCompletion(true);
        return 0;
    }
}
