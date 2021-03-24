package zkMapReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @program: Hadoop->zkMapReducer->JobRunnable
 * @description: 用线程模拟多个作业同时执行
 * @author: Mr.黄
 * @create: 2019-11-03 01:11
 **/
public class JobRunnable extends Configured implements Tool {
    // 解决并发问题，信号量
    private volatile static CountDownLatch count = new CountDownLatch(3);
    public static void main(String[] args) {
        try {
            ToolRunner.run(new JobRunnable(), args);
            /*
            System.exit(ToolRunner.run(
                    new JobRunnable(), args));
            */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        System.out.println("测试练习conf：\t" + conf);
        Thread t1 = new Thread(new ZkDriver(conf));
//        Thread t2 = new Thread(new ZkDriver(conf));
//        Thread t3 = new Thread(new ZkDriver(conf));
        t1.start();
//        t2.start();
//        t3.start();
        return 0;
    }
}
