package test;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->test->TestZk
 * @description:
 * @author: Mr.黄
 * @create: 2019-11-03 16:54
 **/
public class TestZk implements Runnable {
    // 控制zookeeper连接对象的创建，创建连接时异步操作
    private CountDownLatch zkCount = new CountDownLatch(1);
    // 控制异步创建临时节点
    private CountDownLatch creatCount = new CountDownLatch(1);
    // 控制异步操作获取所有子节点名字
    private CountDownLatch getChildrenCount = new CountDownLatch(1);
    // 控制作业执行顺序，按照临时节点编号由大到小执行，执行完断开zookeeper的连接，临时节点自动删除
    private CountDownLatch count = new CountDownLatch(1);
    // 控制全局，所有临时节点的创建，有多少个线程则设置多少
    private CountDownLatch key;
    private ZooKeeper zk;
    private String nodePath;
    private List<String> childrenName;

    @Override
    public void run() {
        // 在zookeeper上创建临时节点
        create("/hhp/test", "test");
        try {
            // 临时节点创建成功
            key.await();

            while (true) {
                // 获取所有子节点
                getChildren("/hhp");
                count.await();
                Thread.sleep(2000);
                System.out.println(Thread.currentThread().getName() + "哈哈哈啊哈");
                Collections.sort(childrenName);
                String[] str = nodePath.split("/");
                String nodeName = str[str.length - 1];
                if (nodeName.equals(childrenName.get(childrenName.size() - 1))) {
                    System.out.println(Thread.currentThread().getName() + nodeName);
                    System.out.println(Thread.currentThread().getName() + "\t结束！！！");

                    zk.close();
                    break;
                } else {
                    count = new CountDownLatch(1);
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 创建连接
    public TestZk(CountDownLatch key) {
        this.key = key;
        try {
            zk = new ZooKeeper("master:2181",
                    5000, new Watcher() {
                // 当前的Watcher监听zk的创建，当zk创建成功
                // 直接调用监听中的方法process()
                // 参数是事件对象
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        System.out.println("连接成功！！！");
                    }
                    // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
                    zkCount.countDown();
                }
            });
            // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
            zkCount.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    // 异步创建节点
    public void create(String path, String value) {
        // 创建连接
        /*
         * 1.创建连接对象
         * 参数1：所有连接的IP:prot，多个ip:port顺序尝试连接，第一个连不上尝试第二个，以此类推
         * 参数2：超时时间，单位为毫秒
         * 参数3：监听（回调函数：zk创建成功的时候会调用第三个参数指定的方法）
         * 注意：连接对象ZooKeeper的构建是异步操作
         */
        // 回调方法
        AsyncCallback.StringCallback as = new AsyncCallback.StringCallback() {
            // 参数1：执行原子操作的状态值
            // 参数2：创建节点的路径
            // 参数3：主函数或create传输的数据
            // 参数4：操作节点的名字
            @Override
            public void processResult(int rc, String path,
                                      Object ctx, String name) {
                if (KeeperException.Code.get(rc) == KeeperException.Code.OK) {
                    System.out.println("节点创建成功");
                    nodePath = name;
                }
                creatCount.countDown();
            }
        };
        // 异步创建节点
        zk.create(path, value.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL, as, null);

        try {
            creatCount.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        key.countDown();
    }

    // 异步获取子节点
    public void getChildren(String path) {
        // 回调方法
        AsyncCallback.ChildrenCallback ac = new AsyncCallback.ChildrenCallback() {
            // 参数1：执行原子参数的状态值
            // 参数2：创建节点的路径
            // 参数3：主函数或原子操作传递的数据信息
            // 参数4：表示指定节点的所有子节点列表
            @Override
            public void processResult(int rc, String path,
                                      Object ctx,
                                      List<String> children) {
                if (KeeperException.Code.get(rc) == KeeperException.Code.OK) {
                    childrenName = children;
                }
                getChildrenCount.countDown();
//                count.countDown();

            }
        };
        // 异步获取子节点
        zk.getChildren(path, false, ac, null);
        try {
            getChildrenCount.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        count.countDown();
    }
}
