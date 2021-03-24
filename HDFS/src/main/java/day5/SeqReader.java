package day5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: Hadoop->day5->SeqReader
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-15 09:47
 **/

// hadoop jar HDFS-1.0-SNAPSHOT.jar day5.SeqReader -D input=/b.txt
public class SeqReader extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new SeqReader(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 获取参数
        String input = conf.get("input");

        // 指定读取的文件路径
        SequenceFile.Reader.Option file =
                SequenceFile.Reader.file(
                        new Path(input));
        // 创建读取对象流
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf, file);
            // 获取key的类型
            Writable key = (Writable) reader.getKeyClass().newInstance();
            // 获取value的类型
            Writable value = (Writable) reader.getValueClass().newInstance();
            // 基于当前的位置找到下一个同步标记
            reader.seek(reader.getPosition());

            // 获取同步标记数量
            int count = 0;
//            System.out.println("compression5");
            System.out.println("compression4");

            // next()方法，一个键值对一个键值对的取
            while (reader.next(key, value)) {
                System.out.println("isCompressed：" + reader.isCompressed());
                System.out.println("isBlockCompressed：" + reader.isBlockCompressed());
                // 判断是否是同步标记
                if (reader.syncSeen()) {
                    count++;
                    System.out.println(key + "--" + value);
                    reader.seek(reader.getPosition());
                }
            }
            System.out.println("同步标记数量：" + count);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) reader.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return 0;
    }
}
