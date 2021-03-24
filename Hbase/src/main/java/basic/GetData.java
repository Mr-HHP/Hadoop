package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: Hadoop->basic->GetData
 * @description: 获取表中数据
 * @author: Mr.黄
 * @create: 2019-11-13 09:37
 **/
public class GetData {
    public static void main(String[] args) {
        Connection conn = null;

        // 1.构建连接对象
        Configuration conf = HBaseConfiguration.create();
        // 设置Hbase集群入口
        conf.set("hbase.zookeeper.quorum", "master:2181");

        try {
            // 使用线程池构建连接对象
            ExecutorService es = Executors.newFixedThreadPool(5);
            // 获取连接对象
            conn = ConnectionFactory.createConnection(conf, es);
            // 获取表对象
            Table table = conn.getTable(TableName.valueOf("briup:emp"));
            // 获取某一行对象
            String row = "1000";
            Get get = new Get(row.getBytes());
            // 获取数据集
            Result result = table.get(get);

            // 通过数据集，获取嵌套的Map集合，遍历数据
            NavigableMap<byte[], NavigableMap<
                    byte[], NavigableMap<
                    Long, byte[]>>> map = result.getMap();
            for (Map.Entry<byte[], NavigableMap<
                    byte[], NavigableMap<
                    Long, byte[]>>> m : map.entrySet()) {
                // 列族
                String cf_key = new String(m.getKey());
                // 表示集合（列名）
                NavigableMap<
                        byte[], NavigableMap<
                        Long, byte[]>> qv = m.getValue();

                for (Map.Entry<byte[], NavigableMap<
                        Long, byte[]>> m1 : qv.entrySet()) {
                    // 标识（列名）
                    String qualifiler_key = new String(m1.getKey());
                    NavigableMap<Long, byte[]> data_value = m1.getValue();

                    for (Map.Entry<Long, byte[]> m2 : data_value.entrySet()) {
                        Long version = m2.getKey();
                        String data = new String(m2.getValue());
                        // 打印数据
                        System.out.println("行键：" + row +
                                "\t列族：" + cf_key +
                                "\t标识（列名）：" + qualifiler_key +
                                "\t版本：" + version +
                                "\t数据值：" + data);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭资源
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
