package day5;

import mkdir.SecondHdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: Hadoop->day5->SeqWriter
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-14 16:29
 **/

// hadoop jar HDFS-1.0-SNAPSHOT.jar day5.SeqWriter -D output=/b.txt
public class SeqWriter extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new SeqWriter(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 获取参数
        String output = conf.get("output");
        FileSystem fs = null;
        SequenceFile.Writer.Option file = null;
        SequenceFile.Writer.Option key = null;
        SequenceFile.Writer.Option value = null;
        SequenceFile.Writer.Option compression = null;
        SequenceFile.Writer writer = null;
        try {
            // 获取hdfs文件系统
            fs = FileSystem.get(conf);
            // 写入文件的位置
            file = SequenceFile.Writer.file(new Path(output));
            // 设置key的类型
            key = SequenceFile.Writer.keyClass(IntWritable.class);
            // 设置value的类型
            value = SequenceFile.Writer.valueClass(Text.class);
            // 设置压缩类型和格式
            // 设置了压缩格式，读取不需要解压，程序自动解压

            /*
            // 值压缩
            compression = SequenceFile.Writer.compression(
                    SequenceFile.CompressionType.RECORD,
                    new BZip2Codec());
            */

            /*
            // 块压缩
            SequenceFile.Writer.Option compression1 = SequenceFile.Writer.compression(
                    SequenceFile.CompressionType.BLOCK,
                    new BZip2Codec());
            System.out.println(compression1);
            */


            // 不进行压缩
            SequenceFile.Writer.Option compression2 = SequenceFile.Writer.compression(
                    SequenceFile.CompressionType.NONE,
                    new BZip2Codec());


            // 一个参数
            SequenceFile.Writer.Option compression3 = SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE);
            SequenceFile.Writer.Option compression4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK);
            SequenceFile.Writer.Option compression5 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);


            // 创建写入器/写入流
//            writer = SequenceFile.createWriter(conf, file, key, value, compression);
//            writer = SequenceFile.createWriter(conf, file, key, value, compression1);
//            writer = SequenceFile.createWriter(conf, file, key, value, compression2);
//            writer = SequenceFile.createWriter(conf, file, key, value, compression3);
            writer = SequenceFile.createWriter(conf, file, key, value, compression4);
//            writer = SequenceFile.createWriter(conf, file, key, value, compression5);

//            System.out.println("getCompressionCodec：" + writer.getCompressionCodec().getCompressorType().getClass().getName());

            for (int i = 1; i <= 10; i++) {
                // 写入同步标记
                if (i % 3 == 0) {
                    writer.sync();
                }
                // 写入数据
                writer.append(new IntWritable(i), new Text("test" + i));
//                    writer.append(i, "&&&" + i);
                System.out.println("blockSize：" + writer.blockSize(i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) writer.close();
                if (fs != null) fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }
}
