package db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.IOException;

/*
把对象写入数据库表中
1.该对象必须是reduce的key输出
 */

// 执行命令：yarn jar MyMapReduce-1.0-SNAPSHOT.jar db.HdfsToDB -D input=/teacher.txt -libjars mysql-connector-java-5.1.39.jar
public class HdfsToDB extends Configured implements Tool {
    static class HdfsToDBMapper extends Mapper<LongWritable, Text, Teacher, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[] = value.toString().split(",");
            Teacher tea = new Teacher();
//            tea.setId(Long.parseLong(str[0].trim()));
            tea.setName(str[1]);
            tea.setAge(Integer.parseInt(str[2].trim()));
            context.write(tea, NullWritable.get());
        }
    }

    static class HdfsToDBReduce extends Reducer<Teacher, NullWritable, Teacher, NullWritable> {
        @Override
        protected void reduce(Teacher key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapredcue.map.java.opts", "-Xmx512m");
        conf.set("mapredcue.reduce.java.opts", "-Xmx512m");
        String input = conf.get("input");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("hdfs_to_db");

        // 测试，检查两个对象是否一样
        System.out.println("conf和job.getConfiguration是否一样：\t" + conf.equals(job.getConfiguration()));
//        System.out.println("job.getConfiguration：\t" + job.getConfiguration());

        //设置链接要素
        /*
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.140:3306/briup",
                "root","root");
        */
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.140:3306/briup",
                "root", "root");

        /*
        insert into teacher(id,name,age) values(?,?,?)
         */
        /*
        DBOutputFormat.setOutput(job,"teacher",
                "id","name","age");
        */

        // 设置SQL语句
        DBOutputFormat.setOutput(job, "tea",
                "name", "age");

        job.setMapperClass(HdfsToDBMapper.class);
        job.setMapOutputKeyClass(Teacher.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(HdfsToDBReduce.class);
        job.setOutputKeyClass(Teacher.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);


        TextInputFormat.addInputPath(job, new Path(input));


        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) {
        try {
            System.exit(
                    new ToolRunner().run(
                            new HdfsToDB(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
