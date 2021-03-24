package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/*
 * @program: Hadoop->basic->APITest
 * @description: Hbase异步操作
 * @author: Mr.黄
 * @create: 2019-11-13 11:19
 */

public class APITest {
    // 配置对象
    private Configuration conf = null;
    // 连接池
    private ExecutorService pool = null;
    // 连接对象
    private Connection conn = null;
    // Admin对象
    private Admin admin = null;
    // Table对象
    private Table table = null;

    // 准备工作
    @Before
    public void before() {
        // 1.构建配置对象
        conf = HBaseConfiguration.create();
        // 设置Hbase集群入口
        conf.set("hbase.zookeeper.quorum", "master:2181");
        // 创建连接池
        pool = Executors.newFixedThreadPool(10);
        try {
            // 使用连接池创建连接对象
            conn = ConnectionFactory.createConnection(conf, pool);
            // 获取Admin对象，进行DDL操作
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 创建命名空间
    @Test
    public void create_namespace() {
        NamespaceDescriptor test =
                NamespaceDescriptor.create("test").build();
        try {
            admin.createNamespace(test);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("命名空间创建成功！！！");
    }

    // 列出所有命名空间
    @Test
    public void list_namespace() {
        try {
            NamespaceDescriptor[] nd = admin.listNamespaceDescriptors();
            // 打印
            for (NamespaceDescriptor n : nd) {
                System.out.println("toString：" + n.toString());
                System.out.println(n.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 列出所有表
    @Test
    public void list_table() {
        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                System.out.println("toString：" + tableName.toString());
//                System.out.println("getName：" + tableName.getName().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 删除命名空间
    @Test
    public void delete_namespace() {
        // 清空命名空间
        try {
            admin.deleteNamespace("test");
            System.out.println("命名空间test删除成功！！！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 同步创建表
    @Test
    public void create_table() {
        // 创建表工厂
        TableDescriptorBuilder builder =
                TableDescriptorBuilder.newBuilder(
                        TableName.valueOf("briup:test"));
        ColumnFamilyDescriptorBuilder cfdb =
                ColumnFamilyDescriptorBuilder.newBuilder(
                        "basicinfo".getBytes());
        ColumnFamilyDescriptor cfd = cfdb.build();
        builder.setColumnFamily(cfd);
        // 通过表工厂创建表描述信息
        TableDescriptor td = builder.build();
        try {
            admin.createTable(td);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("表创建成功！！！");
    }

    // 同步删除表
    @Test
    public void delete_table() {
        System.out.println("删除表！！！");
        System.out.println("开始禁用表！！！");
        try {
            admin.disableTable(TableName.valueOf("briup:test"));
            System.out.println("禁用表成功！！！");
            System.out.println("开始删除！！！");
            admin.deleteTable(TableName.valueOf("briup:test"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("删除成功！！！");
    }

    // 异步创建表
    @Test
    public void asy_createTable() {
        TableDescriptorBuilder builder =
                TableDescriptorBuilder.newBuilder(
                        TableName.valueOf("briup:teacher"));
        ColumnFamilyDescriptorBuilder cfdb =
                ColumnFamilyDescriptorBuilder.newBuilder(
                        "teacher_info".getBytes());
        ColumnFamilyDescriptor cf = cfdb.build();
        builder.setColumnFamily(cf);
        TableDescriptor td = builder.build();
        try {
            // 第二个参数：预分区
            Future<Void> future = admin.createTableAsync(td,
                    new byte[][]{
                            "10".getBytes(),
                            "20".getBytes(),
                            "30".getBytes()});
            System.out.println("等待结果。。。");
            future.get(10000, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } finally {
            System.out.println("创建成功！！！");
        }
    }

    // 异步删除表
    @Test
    public void asyc_deleteTable() {
        System.out.println("开始删除表！！！");
        System.out.println("让表失效！！！");

        try {
            Future<Void> future1 =
                    admin.disableTableAsync(
                            TableName.valueOf("briup:teacher"));
            future1.get(10000, TimeUnit.MILLISECONDS);
            System.out.println("禁用表成功，表已失效！！！");
            Future<Void> future =
                    admin.deleteTableAsync(
                            TableName.valueOf("briup:teacher"));
            future.get(10000, TimeUnit.MILLISECONDS);
            System.out.println("删除表成功！！！");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    // 插入数据
    @Test
    public void putData() {
        try {
            // 获取插入表
            table = conn.getTable(TableName.valueOf("briup:employ"));
            // 设置行键
            Put put = new Put("1000".getBytes());
            // 设置插入的数据
            put.addColumn(
                    "extinfo".getBytes(),
                    "name".getBytes(),
                    "hhp".getBytes());
            put.addColumn(
                    "extinfo".getBytes(),
                    "age".getBytes(),
                    "22".getBytes());
            put.addColumn(
                    "extinfo".getBytes(),
                    "name".getBytes(),
                    "lyf".getBytes());
            put.addColumn(
                    "extinfo".getBytes(),
                    "age".getBytes(),
                    "30".getBytes());
            this.table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("数据插入成功！！！");
    }

    // 获取数据
    @Test
    public void getData() {
        try {
            // 获取表
            table = conn.getTable(TableName.valueOf("briup:employ"));
            Get get = new Get("1234".getBytes());
            Result result = table.get(get);
            long time = table.getReadRpcTimeout(TimeUnit.MILLISECONDS);
            System.out.println("read超时时间：" + time);
            showResult(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void showResult(Result result) {

        NavigableMap<byte[], byte[]> qvMap =
                result.getFamilyMap("info".getBytes());
        String row = Bytes.toString(result.getRow());
        for (Map.Entry<byte[], byte[]> m : qvMap.entrySet()) {
            System.out.println(
                    "行键：" + row +
                            "列族：info" +
                            "\t标识：" + Bytes.toString(m.getKey()) +
                            "\t值：" + Bytes.toString(m.getValue()));
        }
        NavigableMap<
                byte[], NavigableMap<
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
                    System.out.println("行键：" + "1234" +
                            "\t列族：" + cf_key +
                            "\t标识（列名）：" + qualifiler_key +
                            "\t版本：" + version +
                            "\t数据值：" + data);
                }
            }
        }


    }

    // scan 查询整张表操作
    @Test
    public void scan_table() {
        try {
            // 获取要操作的表
            table = conn.getTable(TableName.valueOf("briup:employ"));
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
            int count = 0;
            for (Result result : rs) {
                count++;
                showResult(result);
            }
            System.out.println("===========" + count + "==========");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询表操作，获取其中的列
    @Test
    public void scanTable() {
        // 获取要操作的表
        try {
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            scan.addColumn("info".getBytes(), "name".getBytes());
            for (Result result : results) {
                showResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询表操作，设置隔离级别，获取用户已经提交的数据
    @Test
    public void scanTable1() {
        try {
            // 要操作的表
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                showResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询表操作，限定查询的行数
    @Test
    public void scanTableLimit() {
        try {
            // 要查询的表
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            scan.setLimit(2);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                showResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询表操作，设置只扫描行键的过滤器
    @Test
    public void scanTableRow() {
        try {
            // 要操作的表
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            Filter filter = new KeyOnlyFilter();
            scan.setFilter(filter);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                showResult(result);
            }

//            for (Result result : table.getScanner(scan)) {
//                System.out.println(Bytes.toString(result.getRow()));
//            }


            Iterator<Result> iterator = table.getScanner(scan).iterator();
            while (iterator.hasNext()) {
                System.out.println(
                        Bytes.toString(
                                iterator.next().getRow()));
            }



        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询操作，设置行键前缀过滤器
    @Test
    public void scanTableFilter() {
        try {
            // 要操作的表
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            Filter filter = new PrefixFilter("9".getBytes());
            scan.setFilter(filter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                showResult(result);
            }
            ;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询表操作，设置随机行过滤器，按比例获取随机行数
    @Test
    public void scanTableFilter1() {
        try {
            // 要操作的表
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            Filter filter = new RandomRowFilter(0.2f);
            scan.setFilter(filter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                showResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // scan 查询表操作，设置行范围过滤器
    @Test
    public void scanTableFilter2() {
        try {
            // 要操作的表
            table = conn.getTable(TableName.valueOf("briup:emp"));
            Scan scan = new Scan();
            List<MultiRowRangeFilter.RowRange> list =
                    new ArrayList<MultiRowRangeFilter.RowRange>();
            list.add(
                    new MultiRowRangeFilter.RowRange(
                            "1000", true,
                            "1234", true));
            Filter filter = new MultiRowRangeFilter(list);
            scan.setFilter(filter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                showResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭资源
    @After
    public void after() {
        try {
            admin.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
