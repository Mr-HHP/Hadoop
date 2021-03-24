package basic;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->basic->WatcherTest
 * @description: 监听练习
 * @author: Mr.黄
 * @create: 2019-11-01 14:56
 **/
public class WatcherTest extends ConnectionZK{
    // 解决并发问题，信号量
    private static CountDownLatch count = new CountDownLatch(1);
    public WatcherTest(String ip_port) {
        super(ip_port);
    }

    public static void main(String[] args) throws InterruptedException {
        WatcherTest watcherTest = new WatcherTest("master:2181");
        // 创建连接对象
        watcherTest.connect();

//        watcherTest.existsWatcher("/path/hhp");
        watcherTest.getChildrenWatcher("/path");

        // 使当前线程在锁存器倒计数为0之前一直等待，除非线程被中断
        count.await();
//        Thread.sleep(Long.MAX_VALUE);

        // 关闭资源
        if (watcherTest.zk != null) {
            watcherTest.close();
        }
    }

    // 同步操作使用监听器监听获取子节点
    public void getChildrenWatcher(String path) {
        // 创建连接
//        connect();
        // 执行原子操作
        try {
            List<String> list = zk.getChildren(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("获取的事件对象类型：" + event.getType());

                }
            });
            for (String s : list) {
                System.out.println("子节点：" + s);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 同步操作使用监听器判断目录节点是否存在
    public void existsWatcher(String path) {
        // 创建连接
//        connect();
        // 执行原子操作
        try {
            // 创建节点的时候会触发NodeCreated
            // 创建子节点不触发
            // 删除节点触发NodeDeleted
            // 删除子节点不触发
            // 修改节点触发NodeDataChanged
            // 修改子节点不触发
            zk.exists(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 获取事件对象类型
                    System.out.println("获取的事件对象类型：" + event.getType());
                    existsWatcher(path);
                }
            });
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
