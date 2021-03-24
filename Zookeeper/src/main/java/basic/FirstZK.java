package basic;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->basic->FirstZK
 * @description: zookeeper练习
 * @author: Mr.黄
 * @create: 2019-11-01 00:12
 **/
public class FirstZK extends ConnectionZK {
    // 解决并发问题，信号量
    private static CountDownLatch count = new CountDownLatch(1);
    public FirstZK(String ip_port) {
        super(ip_port);
    }

    public static void main(String[] args) {
        // 创建连接
        FirstZK fz = new FirstZK("master:2181");
        // 执行原子操作
//        fz.create_syc("/test/1", "test");
//        fz.getChildren("/hhp");
        fz.getChildren_stat("/test");
//        fz.getChildren_syc("/hhp");
//        fz.getData("/");
//        fz.getData_syc("/test1");
//        fz.setData("/test2", "肚子饿了！！！");
//        fz.setData_syc("/test2", "好烦啊！！");
//        fz.delete("/test2");
//        fz.delete_syc("/jd");
//        fz.exists("/test");
        // 关闭资源
        if (fz.zk != null) {
            fz.close();
        }
    }

    // 同步操作判断节点是否存在
    public void exists(String path) {
        // 创建连接
        connect();
        // 执行原子操作
        try {
            Stat stat = zk.exists(path, false);
            if (stat != null) {
                System.out.println("该节点存在！！！");
                System.out.println("节点元信息：" + stat);
                System.out.println("该节点存在的子节点数量：" + stat.getNumChildren());
                System.out.println("该节点的数据长度：" + stat.getDataLength());
            } else {
                System.out.println("该节点不存在！！！");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 异步删除节点
    public void delete_syc(String path) {
        // 创建连接
        connect();
        // 回调方法
        AsyncCallback.VoidCallback ac = new AsyncCallback.VoidCallback() {
            // 参数1：执行原子操作的状态量
            // 参数2：节点的路径
            // 参数3：主函数或者原子操作回调时传输的数据信息
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.get(rc) == KeeperException.Code.OK) {
                    System.out.println("传输的数据信息：" + ctx);
                    System.out.println("节点删除成功！！！");
                    System.out.println("被删除的节点路径：" + path);
                }
                // 递减锁存器的计数，如果计数到达0，则释放所有等待的线程。
                count.countDown();
            }
        };
        // 执行原子操作
        zk.delete(path, -1, ac, null);
        try {
            // 使当前线程在锁存器倒计数为0之前一直等待，除非线程被中断
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 同步删除节点
    // 如果有多级节点，不能一次性删除，只能一级一级的删除
    public void delete(String path) {
        // 创建连接
        connect();
        // 执行同步原子操作
        try {
            zk.delete(path, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    // 异步插入节点数据
    public void setData_syc(String path, String value) {
        // 创建连接
        connect();
        // 回调方法
        AsyncCallback.StatCallback as = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path,
                                      Object ctx, Stat stat) {
                if (KeeperException.Code.get(rc) == KeeperException.Code.OK) {
                    System.out.println("插入数据节点的元信息：" + stat);
                }
                // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
                count.countDown();
            }
        };
        // 执行异步原子操作
        zk.setData(path, value.getBytes(), -1, as, null);
        try {
            // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 同步插入节点数据
    public void setData(String path, String value) {
        // 创建连接
        connect();
        // 执行同步原子操作
        try {
            // 参数3：-1代表忽略版本号，直接替换最新的数据
            Stat stat = zk.setData(path, value.getBytes(), -1);
            System.out.println("插入数据的节点的元信息" + stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 异步获取节点数据
    public void getData_syc(String path) {
        // 创建连接
        connect();
        // 回调方法
        AsyncCallback.DataCallback ad = new AsyncCallback.DataCallback() {
            // 参数1：执行原子此操作的状态值
            // 参数2：节点路径
            // 参数3：主函数或者原子操作传输的信号
            // 参数4：指定节点的数据
            // 参数5：指定节点的元信息
            @Override
            public void processResult(int rc, String path,
                                      Object ctx, byte[] data,
                                      Stat stat) {
                if (KeeperException.Code.get(rc) == KeeperException.Code.OK) {
                    System.out.println("节点数据：" + new String(data));
                    System.out.println("节点元信息：" + stat);
                    System.out.println("子节点数量：" + stat.getNumChildren());
                    System.out.println("节点数据长度：" + stat.getDataLength());
                }
                // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
                count.countDown();
            }
        };
        // 执行异步原子操作
        zk.getData(path, false, ad, null);
        try {
            // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    // 同步获取节点数据
    public void getData(String path) {
        // 创建连接
        connect();
        Stat stat = new Stat();
        // 执行原子操作
        try {
            // 将空对象Stat传入getData()方法，用来获取指定路径节点的数据
            // 内部已经对Stat对象进行了封装
            byte[] data = zk.getData(path, false, stat);
            System.out.println("节点数据：" + new String(data));
            System.out.println("节点元信息：" + stat);
            System.out.println("子节点数量：" + stat.getNumChildren());
            System.out.println("节点数据长度：" + stat.getDataLength());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 异步操作获取子节点名字
    public void getChildren_syc(String path) {
        // 创建连接
        connect();
        // 回调方法
        AsyncCallback.Children2Callback ac = new AsyncCallback.Children2Callback() {
            // 参数1：执行原子参数的状态值
            // 参数2：创建节点的路径
            // 参数3：主函数或原子操作传递的数据信息
            // 参数4：表示指定节点的所有子节点列表
            // 参数5：指定节点的元信息
            @Override
            public void processResult(int rc, String path,
                                      Object ctx, List<String> children,
                                      Stat stat) {
                if (KeeperException.Code.get(rc) == KeeperException.Code.OK) {
                    for (String s : children) {
                        System.out.println("子节点名字：" + s);
                    }
                    System.out.println("===================");
                    System.out.println(stat);
                    System.out.println("子节点数量" + stat.getNumChildren());
                    System.out.println("子节点数据长度：" + stat.getDataLength());
                }
                // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
                count.countDown();
            }
        };
        // 执行原子操作
        // 异步的原子操作
        zk.getChildren(path, false, ac, "My Test");
        try {
            // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 同步操作获取子节点名字（获取元信息）
    public void getChildren_stat(String path) {
        // 创建连接
        connect();
        Stat stat = new Stat();
        // 执行原子操作
        try {
            // 将空对象Stat传入getChildren，用来获取指定路径子节点的元信息
            // 内部已经对Stat对象进行了封装
            List<String> list = zk.getChildren(path, false, stat);
            for (String s : list) {
                System.out.println("子节点名字：" + s);
            }
            System.out.println("子节点数量：" + stat.getNumChildren());
            System.out.println("子节点当前版本：" + stat.getVersion());
            System.out.println("子节点数据长度：" + stat.getDataLength());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 同步操作获取子节点名字（不获取目录元信息）
    public void getChildren(String path) {
        // 创建连接
        connect();
        List<String> list = null;
        // 执行原子操作
        try {
            // 同步操作，方法没有结束调用，主函数不会往下走
            list = zk.getChildren(path, false);
            for (String s : list) {
                System.out.println("子节点：" + s);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 异步操作创建节点
    public void create_syc(String path, String value) {
        // 创建连接
        connect();
        // 回调方法
        AsyncCallback.StringCallback as = new AsyncCallback.StringCallback() {
            // 参数1：执行原子操作的状态值
            // 参数2：创建节点的路径
            // 参数3：主函数或create传输的数据
            // 参数4：操作节点的名字
            @Override
            public void processResult(int rc, String path,
                                      Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case OK:
                        System.out.println("创建节点成功！！！");
                        System.out.println("执行原子操作状态值：" + rc
                                + "\t节点路径：" + path
                                + "\t传输的数据：" + ctx
                                + "\t节点名字：" + name);
                        break;
                    case NODEEXISTS:
                        System.out.println("节点已经存在！！！");
                        System.out.println("执行原子操作状态值：" + rc
                                + "\t节点路径：" + path
                                + "\t传输的数据：" + ctx
                                + "\t节点名字：" + name);
                        break;
                    case NONODE:
                        System.out.println("节点不存在！！！");
                        System.out.println("执行原子操作状态值：" + rc
                                + "\t节点路径：" + path
                                + "\t传输的数据：" + ctx
                                + "\t节点名字：" + name);
                        break;
                    case CONNECTIONLOSS:
                        System.out.println("连接丢失！！！");
                        System.out.println("执行原子操作状态值：" + rc
                                + "\t节点路径：" + path
                                + "\t传输的数据：" + ctx
                                + "\t节点名字：" + name);
                        break;
                    default:
                        System.out.println("其他情况！！！");
                        System.out.println("执行原子操作状态值：" + rc
                                + "\t节点路径：" + path
                                + "\t传输的数据：" + ctx
                                + "\t节点名字：" + name);
                        break;
                }
                // 递减锁存器的计数，如果计数到达零，则释放所有等待的线程。
                count.countDown();
            }
        };
        // 执行原子操作
        // 参数1：创建的节点路径（绝对路径）
        // 参数2：节点内容
        // 创建的节点权限，zookeeper一般不关注权限
        // 创建目录节点的模式（四种模式）
        // 异步操作
        zk.create(path, value.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL, as, "反馈传输的数据！！！");
        // 使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断。
        try {
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
