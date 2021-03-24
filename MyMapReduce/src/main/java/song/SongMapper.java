package song;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @program: Hadoop->song->SongMapper
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-18 09:04
 **/
public class SongMapper extends Mapper<LongWritable, Text, Text, Text> {
    // 存储文件名
    private Text fileName = new Text();

    // 调用map之前初始化的方法
    // 获取文件名
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取map方法处理的切片
        FileSplit fs = (FileSplit) context.getInputSplit();
        fileName.set(fs.getPath().getName().split("\\.")[0]);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 判断文件
        if ("songs".equals(fileName.toString())) {
            // 拆分数据
            String[] line = value.toString().split(" ");
            String out_key = line[0];
            String out_value = line[1] + " " + line[2];
            // 发送
            context.write(new Text(out_key), new Text(out_value));
        } else if ("user".equals(fileName.toString())) {
            // 拆分数据
            String out_key = value.toString().split(" ")[1];
            String out_value = value.toString().split(" ")[0];

            context.write(new Text(out_key), new Text(out_value));
        }
    }
}
