package Sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: Hadoop->Sample->SeqWrite
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-21 11:40
 **/
public class SeqWrite extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new SeqWrite(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf=getConf();
        // 获取参数
        String output = conf.get("output");
        // 获取hdfs文件系统
        FileSystem fs = FileSystem.get(conf);
        // 写入文件位置
        SequenceFile.Writer.Option file = SequenceFile.Writer.file(new Path(output));

        // 设置写入key类型
        SequenceFile.Writer.Option key = SequenceFile.Writer.keyClass(IntWritable.class);
        // 设置写入value类型
        SequenceFile.Writer.Option value = SequenceFile.Writer.valueClass(Text.class);
//        SequenceFile.Writer.Option value = SequenceFile.Writer.valueClass(NullWritable.class);

        // 创建写入器

        SequenceFile.Writer writer = SequenceFile.createWriter(conf, file, key, value);

        // 写入数据
        writer.append(new IntWritable(10), new Text("lisi"));
        writer.append(new IntWritable(20), new Text("hhp"));
        writer.append(new IntWritable(57), new Text("wangwu"));
        writer.append(new IntWritable(26), new Text("zhaoliu"));
        writer.append(new IntWritable(21), new Text("zhangsan"));
        writer.append(new IntWritable(45), new Text("sunqian"));
        writer.append(new IntWritable(18), new Text("zhaoyi"));
        writer.append(new IntWritable(28), new Text("liuzi"));
        writer.append(new IntWritable(), new Text("liuzi"));

//        writer.append(null, new Text("liuzi"));

        /*
        // 读取本地文件
        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream("G:\\a.txt")));
        String line = br.readLine();
        */

        // 关闭资源
        writer.close();
        fs.close();

        return 0;
    }
}
