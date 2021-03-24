package Sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: Hadoop->Sample->SeqReader
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-21 13:57
 **/
public class SampleSeqReader extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new SampleSeqReader(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 获取参数
        String input = conf.get("input");

        // 读取文件位置
        SequenceFile.Reader.Option file =
                SequenceFile.Reader.file(
                        new Path(input));
        // 创建读取流
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, file);
        // 读取key类型
        IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
        // 读取value类型
//        Text value = (Text) reader.getValueClass().newInstance();

        // 打印
        while (reader.next(key)) {
            System.out.println(key);
        }

        return 0;
    }
}
