package test;

import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->test->TestRunnable
 * @description:
 * @author: Mr.黄
 * @create: 2019-11-03 16:53
 **/
public class TestRunnable {
    private static CountDownLatch key = new CountDownLatch(3);
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(new TestZk(key), "t1");
        Thread t2 = new Thread(new TestZk(key), "t2");
        Thread t3 = new Thread(new TestZk(key), "t3");
        t1.start();
        t2.start();
        t3.start();
    }
}
