package db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DbToHdfs extends Configured implements Tool {
    static class DbToHdfsMapper
            extends Mapper<LongWritable,Teacher,LongWritable,Teacher>{
        @Override
        protected void map(LongWritable key, Teacher value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        String output=conf.get("output");
        conf.set("mapreduce.reduce.memory.mb","512");
        conf.set("mapreduce.map.memory.mb","512");
        conf.set("mapredcue.map.java.opts","-Xmx512m");
        conf.set("mapredcue.reduce.java.opts","-Xmx512m");

        // 创建作业
        Job job= Job.getInstance(conf);
        // 设置主方法入口
        job.setJarByClass(this.getClass());
        job.setJobName("db_to_hdfs");
        
        //设置链接要素
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.85.140:3306/briup",
                "root","root");

        /*
        select id,name,age
        from teacher
        where 1=1
        order by name
         */

        // 设置SQL语句
        DBInputFormat.setInput(job,Teacher.class,
                "teacher",
                "1=1","name",
                "id","name","age");

        job.setMapperClass(DbToHdfsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Teacher.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));

        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args)throws Exception {
        System.exit(new ToolRunner().run(new DbToHdfs(),args));
    }
}
