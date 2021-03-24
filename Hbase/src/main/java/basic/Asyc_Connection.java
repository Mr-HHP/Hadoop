package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.*;

/**
 * @program: Hadoop->basic->Asyc_Connection
 * @description:
 * @author: Mr.黄
 * @create: 2019-11-13 15:00
 **/
public class Asyc_Connection {
    public static void main(String[] args) {
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 设置Hbase集去入口
        conf.set("hbase.zookeeper.quorum", "master:2181");
        // 使用异步操作创建连接对象
        CompletableFuture<AsyncConnection> conn =
                ConnectionFactory.createAsyncConnection(conf);
        try {
            // get方法在规定时间从CompletableFuture取连接对象
            // 取到了返回AsyncConnection，取不到直接报错
            AsyncConnection ac = conn.get(30000, TimeUnit.MILLISECONDS);
            // 创建连接池
            ExecutorService pool = Executors.newFixedThreadPool(2);
            // 创建表对象
            AsyncTable<ScanResultConsumer> table =
                    ac.getTable(TableName.valueOf("briup:employ"), pool);
            System.out.println("table对象：" + table);
            // 获取某一行
            String row = "1234";
            Get get = new Get(row.getBytes());
            // 获取数据集
            CompletableFuture<Result> completableFuture = table.get(get);
            Result result =
                    completableFuture.get(30000, TimeUnit.MILLISECONDS);
            System.out.println("result对象：" + result);
            // 遍历数据集
            NavigableMap<byte[], NavigableMap<
                    byte[], NavigableMap<
                    Long, byte[]>>> map = result.getMap();
            for (Map.Entry<byte[], NavigableMap<
                    byte[], NavigableMap<
                    Long, byte[]>>> m : map.entrySet()) {
                // 列族
                String cf_name = new String(m.getKey());
                NavigableMap<
                        byte[], NavigableMap<
                        Long, byte[]>> mValue = m.getValue();
                for (Map.Entry<byte[], NavigableMap<
                        Long, byte[]>> m1 : mValue.entrySet()) {
                    // 标识（列名）
                    String qualifiler = new String(m1.getKey());
                    NavigableMap<Long, byte[]> m1Value = m1.getValue();
                    for (Map.Entry<Long, byte[]> m2 : m1Value.entrySet()) {
                        Long version = m2.getKey();
                        String data = new String(m2.getValue());
                        // 打印数据
                        System.out.println("行键：" + row +
                                "\t列族：" + cf_name +
                                "\t标识（列名）：" + qualifiler +
                                "\t版本：" + version +
                                "\t数据值：" + data);
                    }
                }
            }
//            AsyncAdmin admin = ac.getAdmin(pool);
//            System.out.println(admin);

            // 关闭资源
            ac.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
