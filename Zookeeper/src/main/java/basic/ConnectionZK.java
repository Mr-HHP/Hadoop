package basic;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->basic->ConnectionZK
 * @description: 封装ZooKeeper对象的连接
 * @author: Mr.黄
 * @create: 2019-11-01 00:13
 **/
public class ConnectionZK implements Watcher {
    // 连接超时时间，单位毫秒
    private static final int time_out = 5000;
    // 连接IP:port
    private String ip_port;
    // ZooKeeper对象
    protected ZooKeeper zk;
    // 解决并发问题，信号量
    private CountDownLatch count = new CountDownLatch(1);

    public ConnectionZK(String ip_port) {
        this.ip_port = ip_port;
    }

    // 创建连接对象
    protected void connect() {
        try {
            zk = new ZooKeeper(ip_port, time_out, this);
            // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
            count.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 回调函数
    @Override
    public void process(WatchedEvent event) {
        // 判断ZooKeeper对象是否连接成功
        if (event.getState() == Event.KeeperState.SyncConnected) {
            System.out.println("连接成功！！！");

            // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
            count.countDown();
        }
    }

    // 关闭ZooKeeper连接对象
    protected void close() {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
