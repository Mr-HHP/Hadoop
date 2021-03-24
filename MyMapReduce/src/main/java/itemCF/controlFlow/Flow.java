package itemCF.controlFlow;

import itemCF.inDB.InDBDriver;
import itemCF.step1.Step1Driver;
import itemCF.step2.Step2Driver;
import itemCF.step3.Step3Driver;
import itemCF.step4.Step4Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: Hadoop->itemCF.controlFlow->Flow
 * @description: 流程控制，将所有job串联起来
 * @author: Mr.黄
 * @create: 2019-10-28 15:44
 **/
public class Flow extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(
                    new Flow(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 使用getConf()方法读取配置文件
        Configuration conf = getConf();
        // 1.把job变成受控的job
        ControlledJob job1 = new ControlledJob(conf);
        job1.setJob(Step1Driver.step1(conf));
        ControlledJob job2 = new ControlledJob(conf);
        job2.setJob(Step2Driver.step2(conf));
        ControlledJob job3 = new ControlledJob(conf);
        job3.setJob(Step3Driver.step3(conf));
        ControlledJob job4 = new ControlledJob(conf);
        job4.setJob(Step4Driver.step4(conf));
        ControlledJob job5 = new ControlledJob(conf);
        job5.setJob(InDBDriver.step5(conf));

        // 2.配置依赖
        job5.addDependingJob(job4);
        job4.addDependingJob(job3);
        job3.addDependingJob(job2);
        job2.addDependingJob(job1);

        // 3.构建作业流
        JobControl jc = new JobControl("final_job");
        jc.addJob(job1);
        jc.addJob(job2);
        jc.addJob(job3);
        jc.addJob(job4);
        jc.addJob(job5);

        // 4.把作业流交给线程，基于依赖顺序执行
        Thread thread = new Thread(jc);
        thread.start();

        // 5.打印作业执行过程（日志）
        while (true) {
            for (ControlledJob c : jc.getRunningJobList()) {
                c.getJob().monitorAndPrintJob();
            }
            if (jc.allFinished())
                break;
        }
        return 0;
    }
}
