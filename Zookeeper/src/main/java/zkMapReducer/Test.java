package zkMapReducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @program: Hadoop->PACKAGE_NAME->Test
 * @description:
 * @author: Mr.黄
 * @create: 2019-10-31 15:16
 **/
public class Test {
    public static void main(String[] args) {
//        String[] cmd = {"/bin/sh", "-c", "ls"};
        String cmd = "yarn jar Zookeeper-1.0-SNAPSHOT.jar test.Test";
        InputStream in = null;
        String result = null;
        try {
            Process pro = Runtime.getRuntime().exec(cmd);
            pro.waitFor();
            in = pro.getInputStream();
            BufferedReader read = new BufferedReader(new InputStreamReader(in));
            while ((result = read.readLine()) != null) {
                System.out.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

