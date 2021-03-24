package myTool;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @program: Hadoop->myTool->RandomTeacher
 * @description: 随机生成teacher，用于测试
 * @author: Mr.黄
 * @create: 2019-11-02 20:32
 **/
public class RandomTeacher {
    public static void main(String[] args) {
        Random random = new Random();
        int age;
        try {
            BufferedWriter bw = new BufferedWriter(
                    new FileWriter("G:/teacher.txt"));
            for (int i = 0; i < 1000; i++) {
                StringBuffer sb = new StringBuffer("李四");
                age = random.nextInt(100);
                bw.write(String.valueOf(sb.append(i + "\t" + age)));
                bw.newLine();
                bw.flush();
            }
            bw.flush();
            // 关闭资源
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
