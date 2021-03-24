package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @program: Hadoop->basic->CreateTableTest
 * @description: 简单练习
 * @author: Mr.黄
 * @create: 2019-11-13 00:45
 **/
public class CreateTableTest {
    public static void main(String[] args) {
        // 1.构建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 设置Hbase集群和客户端的映射；设置Hbase入口
        conf.set("hbase.zookeeper.quorum", "master:2181");

        try {
            // 2.获取连接对象
            Connection conn = ConnectionFactory.createConnection(conf);
            // 3.构建admin对象，DDL操作
            Admin admin = conn.getAdmin();
            TableDescriptorBuilder builder =
                    TableDescriptorBuilder.newBuilder(
                            TableName.valueOf("briup:bd1904"));

            ColumnFamilyDescriptorBuilder cfdb =
                    ColumnFamilyDescriptorBuilder.
                            newBuilder("info".getBytes());
            cfdb.setMaxVersions(20);
            ColumnFamilyDescriptor cfd = cfdb.build();

            builder.setColumnFamily(cfd);
            TableDescriptor td = builder.build();
            // 4.进行DDL操作
            admin.createTable(td);
            System.out.println("表创建成功！！！");
            // 关闭资源
            conn.close();
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
