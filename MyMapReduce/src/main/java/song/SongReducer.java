package song;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: Hadoop->song->SongReducer
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-18 09:05
 **/
public class SongReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        /*
        String out_key = "";
        for (Text t : values) {
            out_key += t + "##";
        }
        context.write(key, new Text(out_key));
        */



        // 存储用户姓名
        List<Text> user_name = new ArrayList<Text>();//lisi
        String song_name = null;//gequ1
        String hits = null;
        // 处理数据
        for (Text value : values) {
            String[] data = value.toString().split(" ");
            if (data.length == 1) {
                // 保存用户名
                user_name.add(new Text(value));
            } else {
                // 保存歌曲名
                song_name = data[0];
                hits = data[1];
            }
        }

        // 拼接数据
        if (user_name.size() == 0) {
            context.write(
                    new Text("null " + song_name),
                    new Text(hits));
        } else {
            if (song_name == null) {
                for (Text t : user_name) {
                    context.write(
                            new Text(t.toString() + " null"),
                            new Text(0 + ""));
                }
            } else {
                for (Text t : user_name) {
                    String out_key = t.toString() + " " + song_name;
                    // 发送
                    context.write(
                            new Text(out_key),
                            new Text(hits));
                }
            }
        }




    }
}
