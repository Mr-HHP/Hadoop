package basic;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->basic->ZKTest
 * @description: zookeeper练习
 * @author: Mr.黄
 * @create: 2019-10-31 19:02
 **/
/*
* 1.创建ZooKeeper连接对象
* 2.执行原子操作（使用CountDownLatch类解决并发问题，保证异步操作）
* 3.关闭连接对象*/
public class ZKTest {
    // 解决并发问题，信号量
    private static CountDownLatch count = new CountDownLatch(1);
    public static void main(String[] args) {
        /*
        * 1.创建连接对象
        * 参数1：所有连接的IP:prot，多个ip:port顺序尝试连接，第一个连不上尝试第二个，以此类推
        * 参数2：超时时间，单位为毫秒
        * 参数3：监听（回调函数：zk创建成功的时候会调用第三个参数指定的方法）
        * 注意：连接对象ZooKeeper的构建是异步操作
        */
        try {
            ZooKeeper zk = new ZooKeeper("192.168.85.134:2181",
                    5000, new Watcher() {
                // 当前的Watcher监听zk的创建，当zk创建成功
                // 直接调用监听中的方法process()
                // 参数是事件对象
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("状态：" + event.getState());
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        System.out.println("连接成功...");
                    }
                    // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
                    count.countDown();
                }
            });
            // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
            count.await();
            // 2.执行原子操作，基本所有的原子操作 都有同步和异步两种
            String path = zk.create("/jd", "test".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            System.out.println("&&&&&" + "\t" + path);
            // 3.关闭连接对象
            zk.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
